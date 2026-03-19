import os
import sys
import shutil
import logging
import tempfile
from datetime import date, datetime
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

sys.path.append(os.path.dirname(__file__))

from database import get_connection, get_or_create_sheet, save_products_to_db
from parsers import parse_rommer_sheet
from run_parser import parse_date_from_filename, parse_date_from_excel

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Price Parser API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def get_conn():
    """Хелпер: подключение к БД с понятной HTTP-ошибкой при неудаче."""
    try:
        return get_connection()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Нет подключения к БД: {e}")


# ---------------------------------------------------------------------------
# Эндпоинты — данные
# ---------------------------------------------------------------------------

@app.get("/api/sheets")
def list_sheets():
    """
    Список всех листов со скидкой и датой последней загрузки.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                s.id,
                s.sheet_name,
                s.discount_percent,
                s.is_active,
                MAX(ph.updated_at) AS last_updated
            FROM sheets s
            LEFT JOIN products p ON p.sheet_id = s.id
            LEFT JOIN price_history ph ON ph.product_id = p.id AND ph.is_current = true
            GROUP BY s.id, s.sheet_name, s.discount_percent, s.is_active
            ORDER BY s.sheet_name
        """)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "sheet_name": r[1],
                "discount_percent": float(r[2]) if r[2] is not None else None,
                "is_active": r[3],
                "last_updated": r[4].isoformat() if r[4] else None,
            }
            for r in rows
        ]
    finally:
        conn.close()


@app.get("/api/prices")
def get_prices(
    sheet_id: Optional[int] = None,
    search: Optional[str] = Query(None, description="Поиск по артикулу или названию"),
    limit: int = Query(200, le=2000),
    offset: int = 0,
):
    """
    Текущие цены. Можно фильтровать по листу и искать по тексту.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        where = ["ph.is_current = true"]
        params: list = []

        if sheet_id is not None:
            where.append("s.id = %s")
            params.append(sheet_id)

        if search:
            where.append("(p.article ILIKE %s OR p.name ILIKE %s)")
            params += [f"%{search}%", f"%{search}%"]

        where_sql = "WHERE " + " AND ".join(where)

        cur.execute(f"""
            SELECT
                p.article,
                p.name,
                s.sheet_name,
                ph.price_retail,
                ph.price_discounted,
                ph.discount_applied,
                ph.valid_from,
                ph.updated_at
            FROM price_history ph
            JOIN products p ON ph.product_id = p.id
            JOIN sheets s ON p.sheet_id = s.id
            {where_sql}
            ORDER BY s.sheet_name, p.article
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        rows = cur.fetchall()

        # Общее количество (для пагинации)
        cur.execute(f"""
            SELECT COUNT(*)
            FROM price_history ph
            JOIN products p ON ph.product_id = p.id
            JOIN sheets s ON p.sheet_id = s.id
            {where_sql}
        """, params)
        total = cur.fetchone()[0]

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": [
                {
                    "article": r[0],
                    "name": r[1],
                    "sheet_name": r[2],
                    "price_retail": float(r[3]),
                    "price_discounted": float(r[4]),
                    "discount_applied": float(r[5]),
                    "valid_from": r[6].isoformat() if r[6] else None,
                    "updated_at": r[7].isoformat() if r[7] else None,
                }
                for r in rows
            ],
        }
    finally:
        conn.close()


@app.get("/api/prices/history")
def get_price_history(article: str):
    """
    Полная история цен для конкретного артикула.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                ph.price_retail,
                ph.price_discounted,
                ph.discount_applied,
                ph.valid_from,
                ph.valid_to,
                ph.is_current
            FROM price_history ph
            JOIN products p ON ph.product_id = p.id
            WHERE p.article = %s
            ORDER BY ph.valid_from DESC
        """, (article,))
        rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail=f"Артикул {article} не найден")
        return [
            {
                "price_retail": float(r[0]),
                "price_discounted": float(r[1]),
                "discount_applied": float(r[2]),
                "valid_from": r[3].isoformat() if r[3] else None,
                "valid_to": r[4].isoformat() if r[4] and r[4].year < 9999 else None,
                "is_current": r[5],
            }
            for r in rows
        ]
    finally:
        conn.close()


@app.get("/api/prices/compare")
def compare_prices(
    date_from: str = Query(..., description="Дата A в формате YYYY-MM-DD"),
    date_to: str = Query(..., description="Дата B в формате YYYY-MM-DD"),
    sheet_id: Optional[int] = None,
):
    """
    Сравнивает цены на две даты. Возвращает только изменившиеся позиции.
    """
    try:
        d_from = date.fromisoformat(date_from)
        d_to = date.fromisoformat(date_to)
    except ValueError:
        raise HTTPException(status_code=400, detail="Неверный формат даты. Используйте YYYY-MM-DD")

    conn = get_conn()
    try:
        cur = conn.cursor()

        sheet_filter = "AND s.id = %s" if sheet_id else ""
        sheet_params = [sheet_id] if sheet_id else []

        def fetch_prices_on_date(d: date):
            cur.execute(f"""
                SELECT p.article, p.name, s.sheet_name, ph.price_retail, ph.price_discounted
                FROM price_history ph
                JOIN products p ON ph.product_id = p.id
                JOIN sheets s ON p.sheet_id = s.id
                WHERE ph.valid_from <= %s
                  AND (ph.valid_to >= %s OR ph.valid_to IS NULL)
                  {sheet_filter}
                ORDER BY p.article
            """, [d, d] + sheet_params)
            return {r[0]: r for r in cur.fetchall()}

        prices_from = fetch_prices_on_date(d_from)
        prices_to = fetch_prices_on_date(d_to)

        all_articles = set(prices_from) | set(prices_to)
        result = []

        for article in sorted(all_articles):
            a = prices_from.get(article)
            b = prices_to.get(article)

            if a and b and float(a[3]) == float(b[3]):
                continue  # цена не изменилась — пропускаем

            result.append({
                "article": article,
                "name": (b or a)[1],
                "sheet_name": (b or a)[2],
                "price_from": float(a[3]) if a else None,
                "price_discounted_from": float(a[4]) if a else None,
                "price_to": float(b[3]) if b else None,
                "price_discounted_to": float(b[4]) if b else None,
                "change": "new" if not a else ("removed" if not b else "changed"),
            })

        return {
            "date_from": date_from,
            "date_to": date_to,
            "total_changed": len(result),
            "items": result,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Эндпоинт — загрузка прайса
# ---------------------------------------------------------------------------

@app.post("/api/upload")
async def upload_price(
    file: UploadFile = File(...),
    sheet_name: str = Query(..., description="Имя листа в Excel"),
    discount_percent: float = Query(..., description="Скидка в процентах"),
    price_date: Optional[str] = Query(None, description="Дата прайса YYYY-MM-DD (опционально)"),
):
    """
    Принимает xlsx-файл, парсит нужный лист и сохраняет цены в БД.
    """
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Принимаются только .xlsx / .xls файлы")

    # Сохраняем временный файл
    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, file.filename)
    try:
        with open(tmp_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        # Определяем дату
        if price_date:
            try:
                parsed_date = date.fromisoformat(price_date)
            except ValueError:
                raise HTTPException(status_code=400, detail="Неверный формат даты")
        else:
            parsed_date = (
                parse_date_from_filename(file.filename)
                or parse_date_from_excel(tmp_path)
                or date.today()
            )

        # Парсим и сохраняем
        conn = get_conn()
        try:
            sheet_id = get_or_create_sheet(conn, sheet_name, discount_percent)
            products = parse_rommer_sheet(tmp_path, sheet_name, sheet_id, discount_percent)

            if not products:
                raise HTTPException(status_code=422, detail="Товары не найдены. Проверьте имя листа.")

            stats = save_products_to_db(conn, products, parsed_date)
            conn.commit()

            return {
                "success": True,
                "filename": file.filename,
                "sheet_name": sheet_name,
                "price_date": parsed_date.isoformat(),
                "stats": stats,
            }
        except HTTPException:
            raise
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при обработке файла: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            conn.close()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Фронтенд (отдаём HTML прямо из FastAPI)
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index():
    html_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    if os.path.exists(html_path):
        with open(html_path, encoding="utf-8") as f:
            return f.read()
    return HTMLResponse("<h1>Frontend not found</h1><p>Put index.html in scripts/static/</p>", status_code=404)


# ---------------------------------------------------------------------------
# Health check (Render использует его для проверки живости сервиса)
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}