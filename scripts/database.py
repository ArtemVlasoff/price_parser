import psycopg2
from psycopg2.extras import execute_values
from datetime import timedelta, date, datetime
from config import NEON_DB_URL
import logging

logger = logging.getLogger(__name__)

FOREVER_DATE = date(9999, 12, 31)


def get_connection():
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        return conn
    except Exception as e:
        logger.error(f"Не удалось подключиться к БД: {e}")
        raise


# ── Suppliers ─────────────────────────────────────────────────────────────────

def get_all_suppliers(conn) -> list[dict]:
    """Все поставщики с агрегированной информацией по листам."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            sup.id,
            sup.name,
            sup.supplier_type,
            sup.is_active,
            COUNT(DISTINCT s.id)                                        AS sheets_count,
            MAX(ph.updated_at)                                          AS last_updated,
            COUNT(DISTINCT p.id) FILTER (WHERE ph.is_current = true)   AS product_count
        FROM suppliers sup
        LEFT JOIN sheets s   ON s.supplier_id = sup.id
        LEFT JOIN products p ON p.sheet_id = s.id
        LEFT JOIN price_history ph ON ph.product_id = p.id
        GROUP BY sup.id, sup.name, sup.supplier_type, sup.is_active
        ORDER BY sup.supplier_type DESC, sup.name
    """)
    cols = ['id', 'name', 'supplier_type', 'is_active',
            'sheets_count', 'last_updated', 'product_count']
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_or_create_supplier(conn, name: str, supplier_type: str = 'flat') -> int:
    """Возвращает id поставщика, создаёт если нет. БЕЗ коммита."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM suppliers WHERE name = %s", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO suppliers (name, supplier_type) VALUES (%s, %s) RETURNING id",
        (name, supplier_type)
    )
    return cur.fetchone()[0]


# ── Sheets ────────────────────────────────────────────────────────────────────

def get_sheets_by_supplier(conn, supplier_id: int) -> list[dict]:
    """Листы конкретного поставщика."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            s.id,
            s.sheet_name,
            s.discount_percent,
            s.is_active,
            MAX(ph.updated_at)                                         AS last_updated,
            COUNT(DISTINCT p.id) FILTER (WHERE ph.is_current = true)  AS product_count
        FROM sheets s
        LEFT JOIN products p ON p.sheet_id = s.id
        LEFT JOIN price_history ph ON ph.product_id = p.id
        GROUP BY s.id, s.sheet_name, s.discount_percent, s.is_active
        HAVING s.supplier_id = %s
        ORDER BY s.sheet_name
    """, (supplier_id,))
    cols = ['id', 'sheet_name', 'discount_percent', 'is_active', 'last_updated', 'product_count']
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_all_sheets(conn) -> list[dict]:
    """Все листы со supplier_id и агрегацией."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            s.id,
            s.sheet_name,
            s.discount_percent,
            s.is_active,
            s.supplier_id,
            sup.name        AS supplier_name,
            sup.supplier_type,
            MAX(ph.updated_at)                                         AS last_updated,
            COUNT(DISTINCT p.id) FILTER (WHERE ph.is_current = true)  AS product_count
        FROM sheets s
        JOIN suppliers sup ON sup.id = s.supplier_id
        LEFT JOIN products p ON p.sheet_id = s.id
        LEFT JOIN price_history ph ON ph.product_id = p.id
        GROUP BY s.id, s.sheet_name, s.discount_percent, s.is_active,
                 s.supplier_id, sup.name, sup.supplier_type
        ORDER BY sup.name, s.sheet_name
    """)
    cols = ['id', 'sheet_name', 'discount_percent', 'is_active', 'supplier_id',
            'supplier_name', 'supplier_type', 'last_updated', 'product_count']
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def update_sheet_discount(conn, sheet_id: int, discount_percent: float) -> bool:
    cur = conn.cursor()
    cur.execute(
        "UPDATE sheets SET discount_percent = %s WHERE id = %s",
        (discount_percent, sheet_id)
    )
    return cur.rowcount > 0


def update_sheets_discounts_bulk(conn, discounts: dict[int, float]) -> int:
    """Массовое обновление скидок. discounts = {sheet_id: discount_percent}"""
    cur = conn.cursor()
    updated = 0
    for sheet_id, disc in discounts.items():
        cur.execute(
            "UPDATE sheets SET discount_percent = %s WHERE id = %s",
            (disc, sheet_id)
        )
        updated += cur.rowcount
    return updated


def get_or_create_sheet(conn, sheet_name: str, supplier_id: int,
                        default_discount: float = 0) -> int:
    """Возвращает ID листа, создаёт если нет. БЕЗ коммита."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM sheets WHERE sheet_name = %s AND supplier_id = %s",
                (sheet_name, supplier_id))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO sheets (sheet_name, supplier_id, discount_percent) "
        "VALUES (%s, %s, %s) RETURNING id",
        (sheet_name, supplier_id, default_discount)
    )
    return cur.fetchone()[0]


def ensure_sheet_exists(conn, sheet_name: str, supplier_id: int,
                        discount: float = 0.0) -> tuple[int, float]:
    """
    Возвращает (sheet_id, актуальная_скидка).
    Если лист уже есть — берёт скидку из БД.
    Если нет — создаёт с переданной скидкой.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT id, discount_percent FROM sheets "
        "WHERE sheet_name = %s AND supplier_id = %s",
        (sheet_name, supplier_id)
    )
    row = cur.fetchone()
    if row:
        return row[0], float(row[1]) if row[1] is not None else discount
    cur.execute(
        "INSERT INTO sheets (sheet_name, supplier_id, discount_percent) "
        "VALUES (%s, %s, %s) RETURNING id",
        (sheet_name, supplier_id, discount)
    )
    return cur.fetchone()[0], discount


# ── Products + price_history ──────────────────────────────────────────────────

def save_products_to_db(conn, products: list[dict], price_date: date) -> dict:
    """
    Сохраняет товары и цены.

    products — список dict с ключами:
        sheet_id, article, name, price_retail, discount_percent
        code             (опционально)
        price_discounted (опционально — готовая цена из прайса)
    """
    cur = conn.cursor()
    stats = {'new': 0, 'changed': 0, 'unchanged': 0, 'total': len(products)}

    # 1. Upsert товаров
    insert_product_sql = """
        INSERT INTO products (sheet_id, article, code, name)
        VALUES %s
        ON CONFLICT (article) DO UPDATE SET
            sheet_id = EXCLUDED.sheet_id,
            code     = COALESCE(EXCLUDED.code, products.code),
            name     = EXCLUDED.name
        RETURNING id, article
    """
    product_data = [
        (p['sheet_id'], p['article'], p.get('code'), p['name'])
        for p in products
    ]
    product_ids = {}
    for row in execute_values(cur, insert_product_sql, product_data,
                              page_size=1000, fetch=True):
        product_ids[row[1]] = row[0]

    # 2. Текущие цены одним запросом
    cur.execute("""
        SELECT product_id, id, price_retail
        FROM price_history
        WHERE product_id = ANY(%s) AND is_current = true
    """, (list(product_ids.values()),))
    current_prices = {pid: (hid, pr) for pid, hid, pr in cur.fetchall()}

    # 3. Готовим батчи
    products_by_article = {p['article']: p for p in products}
    new_records = []
    updates = []

    for article, product_id in product_ids.items():
        p = products_by_article[article]
        new_price    = p['price_retail']
        new_discount = p.get('discount_percent', 0) or 0

        new_price_discounted = p.get('price_discounted')
        if new_price_discounted is None:
            new_price_discounted = round(new_price * (1 - new_discount / 100), 2)

        if product_id in current_prices:
            history_id, current_price = current_prices[product_id]
            if float(current_price) == float(new_price):
                updates.append(history_id)
                stats['unchanged'] += 1
            else:
                cur.execute("""
                    UPDATE price_history
                    SET valid_to = %s, is_current = false
                    WHERE id = %s
                """, (price_date - timedelta(days=1), history_id))
                now = datetime.now()
                new_records.append((
                    product_id, new_price, new_price_discounted, new_discount,
                    price_date, FOREVER_DATE, True, now, now
                ))
                stats['changed'] += 1
        else:
            now = datetime.now()
            new_records.append((
                product_id, new_price, new_price_discounted, new_discount,
                price_date, FOREVER_DATE, True, now, now
            ))
            stats['new'] += 1

    # 4. Массовые операции
    if updates:
        cur.execute(
            "UPDATE price_history SET updated_at = NOW() WHERE id = ANY(%s)",
            (updates,)
        )
    if new_records:
        execute_values(cur, """
            INSERT INTO price_history
                (product_id, price_retail, price_discounted, discount_applied,
                 valid_from, valid_to, is_current, created_at, updated_at)
            VALUES %s
        """, new_records, page_size=1000)

    logger.info(
        f"💾 Сохранено: всего={stats['total']} "
        f"новых={stats['new']} изменений={stats['changed']} "
        f"без_изменений={stats['unchanged']}"
    )
    return stats