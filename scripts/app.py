import os
import sys
import shutil
import logging
import tempfile
from datetime import date, datetime
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Body
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

sys.path.append(os.path.dirname(__file__))

from database import (
    get_connection, get_or_create_sheet, get_all_sheets,
    update_sheet_discount, ensure_sheet_exists, save_products_to_db,
)
from parsers import (
    parse_flat_sheet, parse_terem_sheet, parse_rommer_spr,
    get_terem_sheets, parse_terem_file,
)
from run_parser import parse_date_from_filename, parse_date_from_excel

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Price Parser API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_conn():
    try:
        return get_connection()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Нет подключения к БД: {e}")

def fmt_sheets(rows: list[dict]) -> list[dict]:
    return [
        {**r,
         "discount_percent": float(r["discount_percent"]) if r["discount_percent"] is not None else 0.0,
         "last_updated": r["last_updated"].isoformat() if r["last_updated"] else None,
         "product_count": r["product_count"] or 0,
        }
        for r in rows
    ]


# ── Sheets ────────────────────────────────────────────────────────────────────

@app.get("/api/sheets")
def list_sheets():
    conn = get_conn()
    try:
        return fmt_sheets(get_all_sheets(conn))
    finally:
        conn.close()


@app.patch("/api/sheets/{sheet_id}")
def patch_sheet_discount(sheet_id: int, discount_percent: float = Body(..., embed=True)):
    """Обновить скидку листа."""
    if not (0 <= discount_percent <= 100):
        raise HTTPException(status_code=400, detail="Скидка должна быть от 0 до 100")
    conn = get_conn()
    try:
        found = update_sheet_discount(conn, sheet_id, discount_percent)
        if not found:
            raise HTTPException(status_code=404, detail="Лист не найден")
        conn.commit()
        return {"ok": True, "sheet_id": sheet_id, "discount_percent": discount_percent}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ── Prices ────────────────────────────────────────────────────────────────────

@app.get("/api/prices")
def get_prices(
    sheet_id: Optional[int] = None,
    search: Optional[str] = Query(None),
    limit: int = Query(200, le=2000),
    offset: int = 0,
):
    conn = get_conn()
    try:
        cur = conn.cursor()
        where = ["ph.is_current = true"]
        params: list = []
        if sheet_id:
            where.append("s.id = %s"); params.append(sheet_id)
        if search:
            where.append("(p.article ILIKE %s OR p.code ILIKE %s OR p.name ILIKE %s)")
            params += [f"%{search}%"] * 3
        w = "WHERE " + " AND ".join(where)
        cur.execute(f"""
            SELECT p.article, p.code, p.name, s.sheet_name,
                   ph.price_retail, ph.price_discounted, ph.discount_applied,
                   ph.valid_from, ph.updated_at
            FROM price_history ph
            JOIN products p ON ph.product_id = p.id
            JOIN sheets s ON p.sheet_id = s.id
            {w} ORDER BY s.sheet_name, p.article
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()
        cur.execute(f"""
            SELECT COUNT(*) FROM price_history ph
            JOIN products p ON ph.product_id = p.id
            JOIN sheets s ON p.sheet_id = s.id {w}
        """, params)
        total = cur.fetchone()[0]
        return {
            "total": total, "limit": limit, "offset": offset,
            "items": [
                {"article": r[0], "code": r[1], "name": r[2], "sheet_name": r[3],
                 "price_retail": float(r[4]), "price_discounted": float(r[5]),
                 "discount_applied": float(r[6]),
                 "valid_from": r[7].isoformat() if r[7] else None,
                 "updated_at": r[8].isoformat() if r[8] else None}
                for r in rows
            ],
        }
    finally:
        conn.close()


@app.get("/api/prices/history")
def get_price_history(article: str):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT ph.price_retail, ph.price_discounted, ph.discount_applied,
                   ph.valid_from, ph.valid_to, ph.is_current
            FROM price_history ph JOIN products p ON ph.product_id = p.id
            WHERE p.article = %s ORDER BY ph.valid_from DESC
        """, (article,))
        rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail=f"Артикул {article} не найден")
        return [
            {"price_retail": float(r[0]), "price_discounted": float(r[1]),
             "discount_applied": float(r[2]),
             "valid_from": r[3].isoformat() if r[3] else None,
             "valid_to": r[4].isoformat() if r[4] and r[4].year < 9999 else None,
             "is_current": r[5]}
            for r in rows
        ]
    finally:
        conn.close()


@app.get("/api/prices/compare")
def compare_prices(
    date_from: str = Query(...),
    date_to: str = Query(...),
    sheet_id: Optional[int] = None,
):
    try:
        d_from = date.fromisoformat(date_from)
        d_to   = date.fromisoformat(date_to)
    except ValueError:
        raise HTTPException(status_code=400, detail="Неверный формат даты (YYYY-MM-DD)")
    conn = get_conn()
    try:
        cur = conn.cursor()
        sf = "AND s.id = %s" if sheet_id else ""
        sp = [sheet_id] if sheet_id else []

        def prices_on(d):
            cur.execute(f"""
                SELECT p.article, p.name, s.sheet_name, ph.price_retail, ph.price_discounted
                FROM price_history ph
                JOIN products p ON ph.product_id = p.id
                JOIN sheets s ON p.sheet_id = s.id
                WHERE ph.valid_from <= %s AND (ph.valid_to >= %s OR ph.valid_to IS NULL) {sf}
            """, [d, d] + sp)
            return {r[0]: r for r in cur.fetchall()}

        pf = prices_on(d_from)
        pt = prices_on(d_to)
        result = []
        for art in sorted(set(pf) | set(pt)):
            a, b = pf.get(art), pt.get(art)
            if a and b and float(a[3]) == float(b[3]):
                continue
            result.append({
                "article": art, "name": (b or a)[1], "sheet_name": (b or a)[2],
                "price_from": float(a[3]) if a else None,
                "price_discounted_from": float(a[4]) if a else None,
                "price_to": float(b[3]) if b else None,
                "price_discounted_to": float(b[4]) if b else None,
                "change": "new" if not a else ("removed" if not b else "changed"),
            })
        return {"date_from": date_from, "date_to": date_to,
                "total_changed": len(result), "items": result}
    finally:
        conn.close()


# ── Upload: плоский прайс ─────────────────────────────────────────────────────

@app.post("/api/upload/flat")
async def upload_flat(
    file: UploadFile = File(...),
    sheet_name: str = Query("", description="Имя листа (пусто = первый лист)"),
    supplier_name: str = Query(..., description="Название поставщика (= имя листа в БД)"),
    discount_percent: float = Query(0.0),
    price_date: Optional[str] = Query(None),
):
    """Загрузка плоского прайса (один лист, Valfex, Импульс и т.п.)."""
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Только .xlsx / .xls")

    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, file.filename)
    try:
        with open(tmp_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        parsed_date = _resolve_date(price_date, file.filename, tmp_path)
        sheet_arg   = sheet_name if sheet_name else 0

        conn = get_conn()
        try:
            sheet_id, actual_discount = ensure_sheet_exists(conn, supplier_name, discount_percent)
            products = parse_flat_sheet(tmp_path, sheet_arg, sheet_id, actual_discount)
            if not products:
                raise HTTPException(status_code=422, detail="Товары не найдены. Проверьте структуру файла.")
            stats = save_products_to_db(conn, products, parsed_date)
            conn.commit()
            return {"success": True, "filename": file.filename,
                    "supplier": supplier_name, "price_date": parsed_date.isoformat(),
                    "stats": stats}
        except HTTPException:
            raise
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка загрузки: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            conn.close()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ── Upload: Терем (multi-sheet) ───────────────────────────────────────────────

@app.get("/api/upload/terem/preview")
async def terem_preview(file: UploadFile = File(...)):
    """
    Принимает файл Терема и возвращает список STOUT/ROMMER листов
    с текущими скидками из БД (или 0 если лист новый).
    Используется для показа формы перед загрузкой.
    """
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Только .xlsx / .xls")
    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, file.filename)
    try:
        with open(tmp_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        sheets_in_file = get_terem_sheets(tmp_path)
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT sheet_name, id, discount_percent FROM sheets WHERE sheet_name = ANY(%s)",
                (sheets_in_file,)
            )
            db_sheets = {r[0]: {"id": r[1], "discount_percent": float(r[2] or 0)} for r in cur.fetchall()}
        finally:
            conn.close()
        return {
            "filename": file.filename,
            "sheets": [
                {"sheet_name": s,
                 "sheet_id": db_sheets[s]["id"] if s in db_sheets else None,
                 "discount_percent": db_sheets[s]["discount_percent"] if s in db_sheets else 0.0,
                 "is_new": s not in db_sheets}
                for s in sheets_in_file
            ]
        }
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@app.post("/api/upload/terem")
async def upload_terem(
    file: UploadFile = File(...),
    price_date: Optional[str] = Query(None),
    sheet_discounts_json: str = Query(default="{}", description="JSON: {sheet_name: discount_percent}"),
):
    """
    Загрузка файла Терема целиком.
    sheet_discounts_json — JSON-строка со скидками по листам.
    """
    import json
    try:
        sheet_discounts = json.loads(sheet_discounts_json)
    except Exception:
        sheet_discounts = {}
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Только .xlsx / .xls")
    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, file.filename)
    try:
        with open(tmp_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        parsed_date = _resolve_date(price_date, file.filename, tmp_path)
        sheets_in_file = get_terem_sheets(tmp_path)

        conn = get_conn()
        try:
            # Получаем / создаём все листы, фиксируем скидки если переданы
            sheet_ids   = {}
            actual_disc = {}
            for s in sheets_in_file:
                disc = sheet_discounts.get(s)  # из формы
                sid, saved_disc = ensure_sheet_exists(conn, s, disc or 0.0)
                sheet_ids[s]   = sid
                # Если скидка передана явно — сохраняем в БД
                if disc is not None:
                    update_sheet_discount(conn, sid, disc)
                    actual_disc[s] = disc
                else:
                    actual_disc[s] = saved_disc

            # Парсим все листы
            all_products = parse_terem_file(tmp_path, actual_disc, sheet_ids)

            total_stats = {'new': 0, 'changed': 0, 'unchanged': 0, 'total': 0}
            sheet_stats = {}
            for sheet_name, products in all_products.items():
                if not products:
                    sheet_stats[sheet_name] = {'total': 0}
                    continue
                st = save_products_to_db(conn, products, parsed_date)
                sheet_stats[sheet_name] = st
                for k in total_stats:
                    total_stats[k] += st[k]

            conn.commit()
            return {
                "success": True, "filename": file.filename,
                "price_date": parsed_date.isoformat(),
                "sheets_processed": len(all_products),
                "total_stats": total_stats,
                "sheet_stats": sheet_stats,
            }
        except HTTPException:
            raise
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка загрузки Терема: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            conn.close()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _resolve_date(price_date_str, filename, file_path) -> date:
    if price_date_str:
        try:
            return date.fromisoformat(price_date_str)
        except ValueError:
            raise HTTPException(status_code=400, detail="Неверный формат даты (YYYY-MM-DD)")
    return (
        parse_date_from_filename(filename)
        or parse_date_from_excel(file_path)
        or date.today()
    )


# ── Static / Health ───────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def index():
    html_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    if os.path.exists(html_path):
        with open(html_path, encoding="utf-8") as f:
            return f.read()
    return HTMLResponse("<h1>Frontend not found</h1>", status_code=404)


@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}