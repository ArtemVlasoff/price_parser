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
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_date_from_filename(filename):
    """
    Пытается извлечь дату из имени файла.
    Поддерживает форматы:
    - Терем__2026-03-18.xlsx (и любые другие префиксы с __ перед датой)
    - YYYY-MM-DD, YYYY_MM_DD, YYYYMMDD
    - YYYY-MM, YYYY_MM (тогда дата = первое число месяца)
    Возвращает объект date или None.
    """
    base = os.path.splitext(filename)[0]  # убираем расширение
    
    # Ищем паттерн с явным разделителем __ перед датой (Терем__2026-03-18)
    # Группа 1: дата в формате YYYY-MM-DD или YYYY_MM_DD
    match = re.search(r'__(\d{4}[-_]\d{2}[-_]\d{2})', base)
    if match:
        date_str = match.group(1).replace('_', '-')
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            pass
    
    # Ищем любой паттерн YYYY-MM-DD в имени
    match = re.search(r'(\d{4})[-_](\d{2})[-_](\d{2})', base)
    if match:
        year, month, day = map(int, match.groups())
        return datetime(year, month, day).date()
    
    # Ищем YYYY-MM или YYYY_MM (для месячных прайсов)
    match = re.search(r'(\d{4})[-_](\d{2})(?![-_]\d{2})', base)  # не захватываем, если есть день
    if match:
        year, month = map(int, match.groups())
        # Берём первое число месяца
        return datetime(year, month, 1).date()
    
    # Ищем просто YYYYMMDD (8 цифр подряд)
    match = re.search(r'(\d{8})', base)
    if match:
        date_str = match.group(1)
        try:
            return datetime.strptime(date_str, '%Y%m%d').date()
        except ValueError:
            pass
    
    return None

def parse_date_from_excel(file_path):
    """
    Пытается извлечь дату из ячейки A1 первого листа Excel.
    Поддерживает даты в текстовом формате и числа Excel.
    Возвращает объект date или None.
    """
    try:
        # Читаем только ячейку A1 первого листа
        df = pd.read_excel(file_path, sheet_name=0, header=None, nrows=1, usecols=[0])
        cell_value = df.iloc[0, 0]
        
        if pd.isna(cell_value):
            logger.debug("Ячейка A1 пуста")
            return None
        
        # Если это уже datetime
        if isinstance(cell_value, (datetime, pd.Timestamp)):
            return cell_value.date()
        
        # Если это число Excel (дни с 1900-01-01)
        if isinstance(cell_value, (int, float)):
            # Excel считает 1900-01-01 за 1, но есть баг с високосным 1900
            try:
                excel_date = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(cell_value) - 2)
                return excel_date.date()
            except:
                pass
        
        # Если это строка, пробуем разные форматы
        if isinstance(cell_value, str):
            # Убираем лишние пробелы
            cell_value = cell_value.strip()
            
            # Пробуем разные форматы дат
            for fmt in ['%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y', '%d.%m.%y', '%Y%m%d']:
                try:
                    return datetime.strptime(cell_value, fmt).date()
                except ValueError:
                    continue
            
            # Если не получилось, пробуем извлечь числа регуляркой
            match = re.search(r'(\d{2})[./-](\d{2})[./-](\d{4})', cell_value)
            if match:
                day, month, year = map(int, match.groups())
                return datetime(year, month, day).date()
            
            match = re.search(r'(\d{4})[./-](\d{2})[./-](\d{2})', cell_value)
            if match:
                year, month, day = map(int, match.groups())
                return datetime(year, month, day).date()
        
        logger.debug(f"Не удалось распознать дату из значения: {cell_value}")
        return None
        
    except Exception as e:
        logger.debug(f"Ошибка при чтении даты из Excel: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Загрузка прайса в базу')
    parser.add_argument('--date', help='Дата прайса в формате ГГГГ-ММ-ДД (например, 2025-01-31)')
    parser.add_argument('--file', default='current.xlsx', help='Имя файла прайса (по умолчанию current.xlsx)')
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("Запуск парсера прайса")
    logger.info("=" * 50)
    
    file_name = args.file
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
    
    # Определяем дату прайса (приоритет: аргумент > имя файла > ячейка A1 > сегодня)
    price_date = None
    
    # 1. Из аргумента командной строки
    if args.date:
        try:
            price_date = datetime.strptime(args.date, '%Y-%m-%d').date()
            logger.info(f"📅 Дата из аргумента: {price_date}")
        except ValueError:
            logger.error("❌ Неверный формат даты. Используйте ГГГГ-ММ-ДД")
            return
    
    # 2. Из имени файла
    if price_date is None:
        extracted_date = parse_date_from_filename(file_name)
        if extracted_date:
            price_date = extracted_date
            logger.info(f"📅 Дата из имени файла: {price_date}")
    
    # 3. Из ячейки A1 первого листа
    if price_date is None:
        excel_date = parse_date_from_excel(file_path)
        if excel_date:
            price_date = excel_date
            logger.info(f"📅 Дата из ячейки A1 Excel: {price_date}")
    
    # 4. Если ничего не помогло, используем сегодня
    if price_date is None:
        price_date = date.today()
        logger.warning(f"⚠️ Дата не найдена, используется сегодняшняя: {price_date}")
    
    # Настройки для конкретного листа
    sheet_name = "Rommer СПР (Россия)"
    discount_percent = 62  # твоя скидка для этого листа
    
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
        
        # Для тестирования берём только первые 30 (раскомментируй когда нужно)
        # products = products[:30]
        # logger.info(f"🔄 Для загрузки выбрано: {len(products)} товаров (первые 30)")
        
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
        # После вызова save_products_to_db
        stats = save_products_to_db(conn, products, price_date)

        # Можно добавить детализацию по категориям, если нужно
        if stats['new'] > 0:
            logger.info(f"  Новые артикулы: {stats['new']}")
        if stats['changed'] > 0:
            logger.info(f"  Изменилось цен: {stats['changed']}")
        if stats['unchanged'] > 0:
            logger.info(f"  Без изменений: {stats['unchanged']}")
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