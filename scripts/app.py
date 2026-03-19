import os
import sys
import json
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
    get_connection,
    get_all_suppliers, get_or_create_supplier,
    get_all_sheets, get_or_create_sheet, ensure_sheet_exists,
    update_sheet_discount, update_sheets_discounts_bulk,
    save_products_to_db,
)
from parsers import (
    parse_flat_sheet, parse_terem_sheet, parse_rommer_spr,
    get_terem_sheets, parse_terem_file,
)
from utils import parse_date_from_filename, parse_date_from_excel

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Price Parser API", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_conn():
    try:
        return get_connection()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Нет подключения к БД: {e}")


def _fmt_row(r: dict) -> dict:
    """Сериализует datetime/Decimal поля."""
    return {
        k: (v.isoformat() if hasattr(v, 'isoformat') else
            float(v) if hasattr(v, '__float__') and not isinstance(v, (int, bool)) else v)
        for k, v in r.items()
    }


def _resolve_date(price_date_str, filename, file_path) -> date:
    if price_date_str:
        try:
            return date.fromisoformat(price_date_str)
        except ValueError:
            raise HTTPException(status_code=400,
                                detail="Неверный формат даты (YYYY-MM-DD)")
    return (parse_date_from_filename(filename)
            or parse_date_from_excel(file_path)
            or date.today())


# ── Suppliers ─────────────────────────────────────────────────────────────────

@app.get("/api/suppliers")
def list_suppliers():
    conn = get_conn()
    try:
        return [_fmt_row(r) for r in get_all_suppliers(conn)]
    finally:
        conn.close()


# ── Sheets ────────────────────────────────────────────────────────────────────

@app.get("/api/sheets")
def list_sheets(supplier_id: Optional[int] = None):
    """
    Все листы. Если передан supplier_id — только листы этого поставщика.
    Возвращает данные сгруппированные для фронтенда.
    """
    conn = get_conn()
    try:
        sheets = get_all_sheets(conn)
        if supplier_id:
            sheets = [s for s in sheets if s['supplier_id'] == supplier_id]
        return [_fmt_row(s) for s in sheets]
    finally:
        conn.close()


@app.patch("/api/sheets/{sheet_id}")
def patch_sheet_discount(sheet_id: int,
                         discount_percent: float = Body(..., embed=True)):
    """Обновить скидку одного листа."""
    if not (0 <= discount_percent <= 100):
        raise HTTPException(status_code=400, detail="Скидка от 0 до 100")
    conn = get_conn()
    try:
        if not update_sheet_discount(conn, sheet_id, discount_percent):
            raise HTTPException(status_code=404, detail="Лист не найден")
        conn.commit()
        return {"ok": True, "sheet_id": sheet_id,
                "discount_percent": discount_percent}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.patch("/api/sheets/bulk")
def patch_sheets_bulk(discounts: dict[int, float] = Body(...)):
    """
    Массовое обновление скидок.
    Body: {"sheet_id": discount_percent, ...}
    """
    for disc in discounts.values():
        if not (0 <= disc <= 100):
            raise HTTPException(status_code=400,
                                detail=f"Скидка должна быть от 0 до 100, получено {disc}")
    conn = get_conn()
    try:
        # Body приходит как {str: float} из JSON — конвертируем ключи в int
        int_discounts = {int(k): v for k, v in discounts.items()}
        updated = update_sheets_discounts_bulk(conn, int_discounts)
        conn.commit()
        return {"ok": True, "updated": updated}
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
    supplier_id: Optional[int] = None,
    search: Optional[str] = Query(None),
    sort_by: Optional[str] = Query(None,
        description="article|name|sheet_name|price_retail|price_discounted|valid_from"),
    sort_dir: Optional[str] = Query("asc", description="asc|desc"),
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
        if supplier_id:
            where.append("sup.id = %s"); params.append(supplier_id)
        if search:
            where.append("(p.article ILIKE %s OR p.code ILIKE %s OR p.name ILIKE %s)")
            params += [f"%{search}%"] * 3

        w = "WHERE " + " AND ".join(where)

        # Сортировка
        allowed_sorts = {
            'article': 'p.article',
            'code': 'p.code',
            'name': 'p.name',
            'sheet_name': 's.sheet_name',
            'supplier_name': 'sup.name',
            'price_retail': 'ph.price_retail',
            'price_discounted': 'ph.price_discounted',
            'valid_from': 'ph.valid_from',
        }
        sort_col = allowed_sorts.get(sort_by, 'sup.name, s.sheet_name, p.article')
        sort_direction = 'DESC' if sort_dir == 'desc' else 'ASC'
        order = f"{sort_col} {sort_direction}"

        cur.execute(f"""
            SELECT p.article, p.code, p.name,
                   s.sheet_name, sup.name AS supplier_name,
                   ph.price_retail, ph.price_discounted, ph.discount_applied,
                   ph.valid_from, ph.updated_at
            FROM price_history ph
            JOIN products p   ON ph.product_id = p.id
            JOIN sheets s     ON p.sheet_id = s.id
            JOIN suppliers sup ON s.supplier_id = sup.id
            {w}
            ORDER BY {order}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        cur.execute(f"""
            SELECT COUNT(*)
            FROM price_history ph
            JOIN products p   ON ph.product_id = p.id
            JOIN sheets s     ON p.sheet_id = s.id
            JOIN suppliers sup ON s.supplier_id = sup.id
            {w}
        """, params)
        total = cur.fetchone()[0]

        return {
            "total": total, "limit": limit, "offset": offset,
            "items": [
                {"article": r[0], "code": r[1], "name": r[2],
                 "sheet_name": r[3], "supplier_name": r[4],
                 "price_retail": float(r[5]), "price_discounted": float(r[6]),
                 "discount_applied": float(r[7]),
                 "valid_from": r[8].isoformat() if r[8] else None,
                 "updated_at": r[9].isoformat() if r[9] else None}
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
            FROM price_history ph
            JOIN products p ON ph.product_id = p.id
            WHERE p.article = %s
            ORDER BY ph.valid_from DESC
        """, (article,))
        rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=404,
                                detail=f"Артикул {article} не найден")
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
    supplier_id: Optional[int] = None,
    sheet_id: Optional[int] = None,
):
    try:
        d_from = date.fromisoformat(date_from)
        d_to   = date.fromisoformat(date_to)
    except ValueError:
        raise HTTPException(status_code=400,
                            detail="Неверный формат даты (YYYY-MM-DD)")
    conn = get_conn()
    try:
        cur = conn.cursor()
        extra_where = ""
        extra_params = []
        if supplier_id:
            extra_where += " AND sup.id = %s"
            extra_params.append(supplier_id)
        if sheet_id:
            extra_where += " AND s.id = %s"
            extra_params.append(sheet_id)

        def prices_on(d):
            cur.execute(f"""
                SELECT p.article, p.name, sup.name, ph.price_retail, ph.price_discounted
                FROM price_history ph
                JOIN products p    ON ph.product_id = p.id
                JOIN sheets s      ON p.sheet_id = s.id
                JOIN suppliers sup ON s.supplier_id = sup.id
                WHERE ph.valid_from <= %s
                  AND (ph.valid_to >= %s OR ph.valid_to IS NULL)
                  {extra_where}
            """, [d, d] + extra_params)
            return {r[0]: r for r in cur.fetchall()}

        pf = prices_on(d_from)
        pt = prices_on(d_to)
        result = []
        for art in sorted(set(pf) | set(pt)):
            a, b = pf.get(art), pt.get(art)
            if a and b and float(a[3]) == float(b[3]):
                continue
            result.append({
                "article": art, "name": (b or a)[1],
                "supplier_name": (b or a)[2],
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
    supplier_name: str = Query(...),
    sheet_name: str = Query(""),
    discount_percent: float = Query(0.0),
    price_date: Optional[str] = Query(None),
):
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
            sup_id   = get_or_create_supplier(conn, supplier_name, 'flat')
            sid, disc = ensure_sheet_exists(conn, supplier_name, sup_id,
                                            discount_percent)
            products = parse_flat_sheet(tmp_path, sheet_arg, sid, disc)
            if not products:
                raise HTTPException(status_code=422,
                                    detail="Товары не найдены")
            stats = save_products_to_db(conn, products, parsed_date)
            conn.commit()
            return {"success": True, "filename": file.filename,
                    "supplier": supplier_name,
                    "price_date": parsed_date.isoformat(), "stats": stats}
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


# ── Upload: Терем ─────────────────────────────────────────────────────────────

@app.get("/api/upload/terem/preview")
async def terem_preview_get(file: UploadFile = File(...)):
    return await _terem_preview(file)


@app.post("/api/upload/terem/preview")
async def terem_preview_post(file: UploadFile = File(...)):
    return await _terem_preview(file)


async def _terem_preview(file: UploadFile):
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
                "SELECT s.sheet_name, s.id, s.discount_percent "
                "FROM sheets s "
                "JOIN suppliers sup ON s.supplier_id = sup.id "
                "WHERE s.sheet_name = ANY(%s) AND sup.supplier_type = 'terem'",
                (sheets_in_file,)
            )
            db_sheets = {r[0]: {"id": r[1], "discount_percent": float(r[2] or 0)}
                         for r in cur.fetchall()}
        finally:
            conn.close()
        return {
            "filename": file.filename,
            "sheets": [
                {"sheet_name": s,
                 "sheet_id": db_sheets[s]["id"] if s in db_sheets else None,
                 "discount_percent": db_sheets[s]["discount_percent"]
                     if s in db_sheets else 0.0,
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
    sheet_discounts_json: str = Query(default="{}"),
):
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
        parsed_date    = _resolve_date(price_date, file.filename, tmp_path)
        sheets_in_file = get_terem_sheets(tmp_path)
        conn = get_conn()
        try:
            # Создаём поставщика Терем если нет
            sup_id = get_or_create_supplier(conn, 'Терем', 'terem')

            sheet_ids, actual_disc = {}, {}
            for s in sheets_in_file:
                disc = sheet_discounts.get(s)
                sid, saved = ensure_sheet_exists(conn, s, sup_id, disc or 0.0)
                sheet_ids[s] = sid
                if disc is not None:
                    update_sheet_discount(conn, sid, disc)
                    actual_disc[s] = disc
                else:
                    actual_disc[s] = saved

            all_products = parse_terem_file(tmp_path, actual_disc, sheet_ids)

            total_stats = {'new': 0, 'changed': 0, 'unchanged': 0, 'total': 0}
            sheet_stats = {}
            for sname, products in all_products.items():
                if not products:
                    sheet_stats[sname] = {'total': 0}
                    continue
                st = save_products_to_db(conn, products, parsed_date)
                sheet_stats[sname] = st
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