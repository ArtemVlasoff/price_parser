import psycopg2
from psycopg2.extras import execute_values
from datetime import timedelta, date
from config import NEON_DB_URL
import logging

logger = logging.getLogger(__name__)

# Константа для "бесконечной" даты
FOREVER_DATE = date(9999, 12, 31)

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
        return sheet_id

def save_products_to_db(conn, products, price_date):
    cur = conn.cursor()
    
    stats = {'new': 0, 'changed': 0, 'unchanged': 0, 'total': len(products)}
    
    # 1. Get or create products and get their IDs
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
    for row in execute_values(cur, insert_product_sql, product_data, page_size=1000, fetch=True):
        product_ids[row[1]] = row[0]
    
    # 2. ОДНИМ ЗАПРОСОМ получаем все текущие цены
    cur.execute("""
        SELECT product_id, id, price_retail 
        FROM price_history 
        WHERE product_id = ANY(%s) AND is_current = true
    """, (list(product_ids.values()),))
    
    # Создаём словарь для быстрого доступа: {product_id: (id, price_retail)}
    current_prices = {}
    for product_id, history_id, price in cur.fetchall():
        current_prices[product_id] = (history_id, price)
    
    # 3. Собираем данные для массовой вставки новых записей
    new_history_records = []
    updates = []
    
    # Создаём словарь для быстрого доступа к данным товаров по артикулу
    product_data_by_article = {p['article']: p for p in products}
    
    for article, product_id in product_ids.items():
        p = product_data_by_article[article]
        new_price = p['price_retail']
        new_discount = p['discount_percent']
        new_price_discounted = round(new_price * (1 - new_discount / 100), 2)
        
        if product_id in current_prices:
            history_id, current_price = current_prices[product_id]
            
            if current_price == new_price:
                # Цена не изменилась — просто запоминаем, что нужно обновить updated_at
                updates.append(history_id)
                stats['unchanged'] += 1
            else:
                # Цена изменилась — закрываем старую
                cur.execute("""
                    UPDATE price_history 
                    SET valid_to = %s, is_current = false
                    WHERE id = %s
                """, (price_date - timedelta(days=1), history_id))
                
                # И добавляем новую запись
                new_history_records.append((
                    product_id, new_price, new_price_discounted, new_discount,
                    price_date, FOREVER_DATE, True
                ))
                stats['changed'] += 1
        else:
            # Новый товар
            new_history_records.append((
                product_id, new_price, new_price_discounted, new_discount,
                price_date, FOREVER_DATE, True
            ))
            stats['new'] += 1
    
    # 4. Массовое обновление updated_at для неизменных цен
    if updates:
        cur.execute("""
            UPDATE price_history 
            SET updated_at = NOW()
            WHERE id = ANY(%s)
        """, (updates,))
    
    # 5. Массовая вставка новых записей
    if new_history_records:
        insert_history_sql = """
            INSERT INTO price_history 
                (product_id, price_retail, price_discounted, discount_applied,
                 valid_from, valid_to, is_current, created_at, updated_at)
            VALUES %s
        """
        execute_values(cur, insert_history_sql, new_history_records, page_size=1000)
    
    logger.info(f"💾 Загрузка завершена: {stats}")
    return stats

def get_current_prices(conn, as_of_date=None):
    """
    Возвращает цены, актуальные на указанную дату.
    Если as_of_date не указана, возвращает текущие цены (valid_to = FOREVER_DATE).
    """
    cur = conn.cursor()
    
    if as_of_date is None:
        # Текущие цены
        query = """
            SELECT 
                p.article,
                p.name,
                s.sheet_name,
                ph.price_retail,
                ph.price_discounted,
                ph.discount_applied,
                ph.valid_from,
                ph.valid_to
            FROM price_history ph
            JOIN products p ON ph.product_id = p.id
            JOIN sheets s ON p.sheet_id = s.id
            WHERE ph.is_current = true
            ORDER BY s.sheet_name, p.article
        """
        cur.execute(query)
    else:
        # Цены на конкретную дату
        query = """
            SELECT 
                p.article,
                p.name,
                s.sheet_name,
                ph.price_retail,
                ph.price_discounted,
                ph.discount_applied,
                ph.valid_from,
                ph.valid_to
            FROM price_history ph
            JOIN products p ON ph.product_id = p.id
            JOIN sheets s ON p.sheet_id = s.id
            WHERE ph.valid_from <= %s 
              AND (ph.valid_to >= %s OR ph.valid_to IS NULL)
            ORDER BY s.sheet_name, p.article
        """
        cur.execute(query, (as_of_date, as_of_date))
    
    return cur.fetchall()