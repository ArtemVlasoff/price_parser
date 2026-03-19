import os
import sys
import logging
import argparse
import re
from datetime import datetime, date
import pandas as pd

# Добавляем путь к папке scripts, чтобы импортировать наши модули
sys.path.append(os.path.dirname(__file__))

from config import PRICE_FILES_DIR
from database import get_connection, get_or_create_sheet, save_products_to_db
from parsers import parse_rommer_sheet

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_date_from_filename(filename):
    """
    Пытается извлечь дату из имени файла.
    Поддерживает форматы: Терем__2026-03-18.xlsx, YYYY-MM-DD, YYYY_MM_DD, YYYYMMDD
    """
    base = os.path.splitext(filename)[0]
    
    # Ищем паттерн с __ перед датой (Терем__2026-03-18)
    match = re.search(r'__(\d{4}[-_]\d{2}[-_]\d{2})', base)
    if match:
        date_str = match.group(1).replace('_', '-')
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            pass
    
    # Ищем YYYY-MM-DD
    match = re.search(r'(\d{4})[-_](\d{2})[-_](\d{2})', base)
    if match:
        year, month, day = map(int, match.groups())
        return datetime(year, month, day).date()
    
    # Ищем YYYYMMDD
    match = re.search(r'(\d{8})', base)
    if match:
        date_str = match.group(1)
        try:
            return datetime.strptime(date_str, '%Y%m%d').date()
        except ValueError:
            pass
    
    return None

def parse_date_from_excel(file_path):
    """Пытается извлечь дату из ячейки A1 первого листа Excel"""
    try:
        df = pd.read_excel(file_path, sheet_name=0, header=None, nrows=1, usecols=[0])
        cell_value = df.iloc[0, 0]
        
        if pd.isna(cell_value):
            return None
        
        if isinstance(cell_value, (datetime, pd.Timestamp)):
            return cell_value.date()
        
        if isinstance(cell_value, str):
            cell_value = cell_value.strip()
            for fmt in ['%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y']:
                try:
                    return datetime.strptime(cell_value, fmt).date()
                except ValueError:
                    continue
        
        return None
    except Exception as e:
        logger.debug(f"Не удалось прочитать дату из Excel: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Загрузка прайса в базу')
    parser.add_argument('--date', help='Дата прайса в формате ГГГГ-ММ-ДД')
    parser.add_argument('--file', default='current.xlsx', help='Имя файла прайса')
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("Запуск парсера прайса")
    logger.info("=" * 50)
    
    file_name = args.file
    file_path = os.path.join(PRICE_FILES_DIR, file_name)
    
    # Проверяем наличие файла
    if not os.path.exists(file_path):
        logger.error(f"❌ Файл не найден: {file_path}")
        return
    
    logger.info(f"✅ Файл найден: {file_path}")
    
    # Определяем дату прайса
    price_date = None
    if args.date:
        try:
            price_date = datetime.strptime(args.date, '%Y-%m-%d').date()
            logger.info(f"📅 Дата из аргумента: {price_date}")
        except ValueError:
            logger.error("❌ Неверный формат даты. Используйте ГГГГ-ММ-ДД")
            return
    
    if price_date is None:
        price_date = parse_date_from_filename(file_name)
        if price_date:
            logger.info(f"📅 Дата из имени файла: {price_date}")
    
    if price_date is None:
        price_date = parse_date_from_excel(file_path)
        if price_date:
            logger.info(f"📅 Дата из Excel: {price_date}")
    
    if price_date is None:
        price_date = date.today()
        logger.warning(f"⚠️ Дата не найдена, используется сегодняшняя: {price_date}")
    
    # Настройки для листа
    sheet_name = "Rommer СПР (Россия)"
    discount_percent = 62
    
    logger.info(f"Лист: {sheet_name}")
    logger.info(f"Скидка: {discount_percent}%")
    logger.info(f"Дата: {price_date}")
    
    # Подключаемся к БД
    logger.info("Подключение к БД...")
    try:
        conn = get_connection()
        logger.info("✅ Подключение успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения: {e}")
        return
    
    # ЕДИНАЯ ТРАНЗАКЦИЯ для всех операций
    try:
        # Получаем или создаём лист (функция БЕЗ коммита)
        sheet_id = get_or_create_sheet(conn, sheet_name, discount_percent)
        logger.info(f"✅ ID листа: {sheet_id}")
        
        # Парсим файл
        products = parse_rommer_sheet(file_path, sheet_name, sheet_id, discount_percent)
        logger.info(f"✅ Найдено товаров: {len(products)}")
        
        if len(products) == 0:
            logger.warning("⚠️ Товары не найдены!")
            return
        
        # Сохраняем (функция БЕЗ коммита)
        stats = save_products_to_db(conn, products, price_date)
        
        # ЕСЛИ ВСЁ ХОРОШО — коммитим одной транзакцией
        conn.commit()
        logger.info("✅ Все данные успешно сохранены в БД")
        logger.info(f"📊 Статистика: новых {stats['new']}, изменений {stats['changed']}, без изменений {stats['unchanged']}")
        
    except Exception as e:
        # ЕСЛИ ОШИБКА — откатываем ВСЁ
        conn.rollback()
        logger.error(f"❌ Ошибка при обработке: {e}", exc_info=True)
        logger.info("⚠️ Транзакция отменена, изменения не сохранены")
    finally:
        conn.close()
        logger.info("Соединение закрыто")
        logger.info("=" * 50)

if __name__ == "__main__":
    main()