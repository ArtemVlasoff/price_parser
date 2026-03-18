import os
import sys
import logging
from datetime import date
import pandas as pd

# Добавляем путь к папке scripts, чтобы импортировать наши модули
sys.path.append(os.path.dirname(__file__))

from config import PRICE_FILES_DIR
from database import get_connection, get_or_create_sheet, save_products_to_db
from parsers import parse_rommer_sheet

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 50)
    logger.info("Запуск парсера прайса")
    logger.info("=" * 50)
    
    # Параметры запуска
    file_name = "current.xlsx"  # или можно передавать как аргумент командной строки
    file_path = os.path.join(PRICE_FILES_DIR, file_name)
    
    logger.info(f"Проверка файла: {file_path}")
    
    # Проверяем наличие файла
    if not os.path.exists(file_path):
        logger.error(f"❌ Файл не найден: {file_path}")
        logger.error(f"Текущая директория: {os.getcwd()}")
        logger.error(f"Содержимое {PRICE_FILES_DIR}:")
        try:
            files = os.listdir(PRICE_FILES_DIR)
            for f in files:
                logger.error(f"  - {f}")
        except FileNotFoundError:
            logger.error(f"  - Папка {PRICE_FILES_DIR} не существует!")
        return
    
    logger.info(f"✅ Файл найден: {file_path}")
    
    # Настройки для конкретного листа
    sheet_name = "Rommer СПР (Россия)"
    discount_percent = 62  # твоя скидка для этого листа
    price_date = date.today()
    
    logger.info(f"Лист для парсинга: {sheet_name}")
    logger.info(f"Скидка: {discount_percent}%")
    logger.info(f"Дата цены: {price_date}")
    
    # Подключаемся к БД
    logger.info("Подключение к БД...")
    try:
        conn = get_connection()
        logger.info("✅ Подключение успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        return
    
    try:
        # Получаем или создаём лист
        logger.info(f"Получение sheet_id для '{sheet_name}'...")
        sheet_id = get_or_create_sheet(conn, sheet_name, discount_percent)
        logger.info(f"✅ ID листа в БД: {sheet_id}")
        
        # Парсим
        logger.info("Начало парсинга файла...")
        products = parse_rommer_sheet(file_path, sheet_name, sheet_id, discount_percent)
        logger.info(f"✅ Найдено товаров: {len(products)}")
        
        if len(products) == 0:
            logger.warning("⚠️ Товары не найдены! Проверь структуру листа")
            return
        
        # Показываем первые 3 товара для проверки
        logger.info("Первые 3 товара для проверки:")
        for product in products[:3]:
            logger.info(f"  - {product['article']}: {product['name']} ({product['price_retail']} руб)")
        
        # Сохраняем
        logger.info("Сохранение товаров в БД...")
        save_products_to_db(conn, products, price_date)
        logger.info("✅ Данные успешно сохранены!")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке: {e}", exc_info=True)
        try:
            conn.rollback()
            logger.info("Транзакция отменена")
        except:
            pass
    finally:
        try:
            conn.close()
            logger.info("Соединение с БД закрыто")
        except:
            pass

if __name__ == "__main__":
    main()