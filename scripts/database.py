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
        conn.commit()
        return sheet_id

def save_products_to_db(conn, products, price_date):
    """
    Сохраняет товары и цены с учётом периодов действия.
    Если цена не изменилась, новая запись НЕ создаётся, а обновляется только updated_at.
    Возвращает словарь со статистикой.
    """
    cur = conn.cursor()
    
    # Статистика
    stats = {
        'new': 0,           # новые товары
        'changed': 0,       # изменилась цена (создана новая запись)
        'unchanged': 0,     # цена не изменилась (обновлён updated_at)
        'total': len(products)
    }
    
    # 1. Получаем ID товаров (создаём новые, если нужно)
    product_ids = {}
    for p in products:
        cur.execute("""
            INSERT INTO products (sheet_id, article, name)
            VALUES (%s, %s, %s)
            ON CONFLICT (article) DO UPDATE SET
                sheet_id = EXCLUDED.sheet_id,
                name = EXCLUDED.name
            RETURNING id
        """, (p['sheet_id'], p['article'], p['name']))
        product_ids[p['article']] = cur.fetchone()[0]
    
    # 2. Для каждого товара определяем судьбу
    for article, product_id in product_ids.items():
        # Находим текущую активную запись
        cur.execute("""
            SELECT id, price_retail, valid_from 
            FROM price_history 
            WHERE product_id = %s AND is_current = true
        """, (product_id,))
        current = cur.fetchone()
        
        # Данные из нового прайса
        product_data = next(p for p in products if p['article'] == article)
        new_price = product_data['price_retail']
        new_discount = product_data['discount_percent']
        new_price_discounted = round(new_price * (1 - new_discount / 100), 2)
        
        if current:
            current_id, current_price, current_valid_from = current
            
            if current_price == new_price:
                # Цена не изменилась - просто обновляем время подтверждения
                cur.execute("""
                    UPDATE price_history 
                    SET updated_at = NOW()
                    WHERE id = %s
                """, (current_id,))
                stats['unchanged'] += 1
                logger.debug(f"⏺ Цена не изменилась: {article}")
            else:
                # Цена изменилась - закрываем старую, создаём новую
                cur.execute("""
                    UPDATE price_history 
                    SET valid_to = %s, is_current = false
                    WHERE id = %s
                """, (price_date - timedelta(days=1), current_id))
                
                cur.execute("""
                    INSERT INTO price_history 
                        (product_id, price_retail, price_discounted, discount_applied, 
                         valid_from, valid_to, is_current, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """, (
                    product_id,
                    new_price,
                    new_price_discounted,
                    new_discount,
                    price_date,
                    FOREVER_DATE,
                    True
                ))
                stats['changed'] += 1
                logger.debug(f"🔄 Цена изменилась: {article} {current_price} → {new_price}")
        else:
            # Новый товар
            cur.execute("""
                INSERT INTO price_history 
                    (product_id, price_retail, price_discounted, discount_applied, 
                     valid_from, valid_to, is_current, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            """, (
                product_id,
                new_price,
                new_price_discounted,
                new_discount,
                price_date,
                FOREVER_DATE,
                True
            ))
            stats['new'] += 1
            logger.debug(f"🆕 Новый товар: {article}")
    
    conn.commit()
    
    # Итоговая статистика
    logger.info(f"💾 Загрузка завершена. "
                f"Всего: {stats['total']}, "
                f"🆕 Новых: {stats['new']}, "
                f"🔄 Изменений: {stats['changed']}, "
                f"⏺ Без изменений (обновлён updated_at): {stats['unchanged']}")
    
    # Если нужно больше деталей, можно добавить:
    if stats['new'] > 0:
        logger.info(f"  Новые артикулы: {stats['new']}")
    if stats['changed'] > 0:
        logger.info(f"  Изменилось цен: {stats['changed']}")
    
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