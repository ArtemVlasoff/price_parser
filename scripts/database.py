import psycopg2
from psycopg2.extras import execute_values
from config import NEON_DB_URL
import logging

logger = logging.getLogger(__name__)

def get_connection():
    """Возвращает соединение с базой данных"""
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        return conn
    except Exception as e:
        logger.error(f"Не удалось подключиться к БД: {e}")
        raise

def get_or_create_sheet(conn, sheet_name, default_discount=0):
    """Возвращает ID листа по его имени"""
    cur = conn.cursor()
    cur.execute("SELECT id FROM sheets WHERE sheet_name = %s", (sheet_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        cur.execute(
            "INSERT INTO sheets (sheet_name, discount_percent) VALUES (%s, %s) RETURNING id",
            (sheet_name, default_discount)
        )
        sheet_id = cur.fetchone()[0]
        conn.commit()
        return sheet_id

def save_products_to_db(conn, products, price_date):
    """Сохраняет список товаров и цен"""
    cur = conn.cursor()
    
    # Вставка/обновление товаров
    insert_product_sql = """
        INSERT INTO products (sheet_id, article, name)
        VALUES %s
        ON CONFLICT (article) DO UPDATE SET
            sheet_id = EXCLUDED.sheet_id,
            name = EXCLUDED.name
        RETURNING id, article
    """
    product_data = [(p['sheet_id'], p['article'], p['name']) for p in products]
    product_ids = {}
    for row in execute_values(cur, insert_product_sql, product_data, page_size=100, fetch=True):
        product_ids[row[1]] = row[0]
    
    # Вставка истории цен
    history_sql = """
        INSERT INTO price_history (product_id, price_date, price_retail, price_discounted, discount_applied)
        VALUES %s
        ON CONFLICT (product_id, price_date) DO NOTHING
    """
    history_data = [
        (product_ids[p['article']], price_date, p['price_retail'],
         round(p['price_retail'] * (1 - p['discount_percent'] / 100), 2),
         p['discount_percent'])
        for p in products
    ]
    execute_values(cur, history_sql, history_data)
    conn.commit()