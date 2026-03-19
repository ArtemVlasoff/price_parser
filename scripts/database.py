import psycopg2
from psycopg2.extras import execute_values
from datetime import timedelta, date
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


# ── Sheets ────────────────────────────────────────────────────────────────────

def get_or_create_sheet(conn, sheet_name: str, default_discount: float = 0) -> int:
    """Возвращает ID листа, создаёт если нет. БЕЗ коммита."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM sheets WHERE sheet_name = %s", (sheet_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO sheets (sheet_name, discount_percent) VALUES (%s, %s) RETURNING id",
        (sheet_name, default_discount)
    )
    return cur.fetchone()[0]


def get_all_sheets(conn) -> list[dict]:
    """Все листы со скидкой и датой последней загрузки."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            s.id,
            s.sheet_name,
            s.discount_percent,
            s.is_active,
            MAX(ph.updated_at) AS last_updated,
            COUNT(DISTINCT p.id) FILTER (WHERE ph.is_current = true) AS product_count
        FROM sheets s
        LEFT JOIN products p ON p.sheet_id = s.id
        LEFT JOIN price_history ph ON ph.product_id = p.id
        GROUP BY s.id, s.sheet_name, s.discount_percent, s.is_active
        ORDER BY s.sheet_name
    """)
    cols = ['id', 'sheet_name', 'discount_percent', 'is_active', 'last_updated', 'product_count']
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def update_sheet_discount(conn, sheet_id: int, discount_percent: float) -> bool:
    """Обновляет скидку листа. Возвращает True если лист найден."""
    cur = conn.cursor()
    cur.execute(
        "UPDATE sheets SET discount_percent = %s WHERE id = %s",
        (discount_percent, sheet_id)
    )
    return cur.rowcount > 0


def ensure_sheet_exists(conn, sheet_name: str, discount: float) -> tuple[int, float]:
    """
    Возвращает (sheet_id, актуальная_скидка).
    Если лист уже есть — берёт скидку из БД (там могла быть выставлена руками).
    Если нет — создаёт с переданной скидкой.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT id, discount_percent FROM sheets WHERE sheet_name = %s", (sheet_name,)
    )
    row = cur.fetchone()
    if row:
        return row[0], float(row[1]) if row[1] is not None else discount
    cur.execute(
        "INSERT INTO sheets (sheet_name, discount_percent) VALUES (%s, %s) RETURNING id",
        (sheet_name, discount)
    )
    return cur.fetchone()[0], discount


# ── Products + price_history ──────────────────────────────────────────────────

def save_products_to_db(conn, products: list[dict], price_date: date) -> dict:
    """
    Сохраняет товары и цены.

    products — список dict с ключами:
        sheet_id, article, name, price_retail, discount_percent
        code (опционально — supplier code)
        price_discounted (опционально — готовая цена из прайса)

    Если price_discounted передан — используем его.
    Иначе считаем: price_retail * (1 - discount_percent / 100).
    """
    cur = conn.cursor()
    stats = {'new': 0, 'changed': 0, 'unchanged': 0, 'total': len(products)}

    # 1. Upsert товаров — теперь включаем поле code
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
    for row in execute_values(cur, insert_product_sql, product_data, page_size=1000, fetch=True):
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

        # Цена со скидкой: из прайса или вычисляем
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
                new_records.append((
                    product_id, new_price, new_price_discounted, new_discount,
                    price_date, FOREVER_DATE, True
                ))
                stats['changed'] += 1
        else:
            new_records.append((
                product_id, new_price, new_price_discounted, new_discount,
                price_date, FOREVER_DATE, True
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
        f"новых={stats['new']} изменений={stats['changed']} без_изменений={stats['unchanged']}"
    )
    return stats