import os
import sys
import logging
import argparse
import re
from datetime import datetime, date
import pandas as pd

sys.path.append(os.path.dirname(__file__))

from config import PRICE_FILES_DIR
from database import get_connection, ensure_sheet_exists, save_products_to_db
from parsers import parse_rommer_spr, parse_terem_file, parse_flat_sheet, get_terem_sheets

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

    match = re.search(r'__(\d{4}[-_]\d{2}[-_]\d{2})', base)
    if match:
        date_str = match.group(1).replace('_', '-')
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            pass

    match = re.search(r'(\d{4})[-_](\d{2})[-_](\d{2})', base)
    if match:
        year, month, day = map(int, match.groups())
        return datetime(year, month, day).date()

    match = re.search(r'(\d{8})', base)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y%m%d').date()
        except ValueError:
            pass

    return None


def parse_date_from_excel(file_path):
    """Пытается извлечь дату из ячейки A1 первого листа Excel."""
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
    parser.add_argument('--date',     help='Дата прайса ГГГГ-ММ-ДД')
    parser.add_argument('--file',     default='current.xlsx', help='Имя файла прайса')
    parser.add_argument('--type',     default='terem',
                        choices=['terem', 'flat'],
                        help='Тип прайса: terem (STOUT/ROMMER) или flat (плоский)')
    parser.add_argument('--supplier', default='', help='Название поставщика (для flat)')
    parser.add_argument('--sheet',    default='', help='Имя листа (для flat; пусто = первый)')
    parser.add_argument('--discount', type=float, default=0.0, help='Скидка % (для flat)')
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("Запуск парсера прайса")
    logger.info("=" * 50)

    file_path = os.path.join(PRICE_FILES_DIR, args.file)
    if not os.path.exists(file_path):
        logger.error(f"❌ Файл не найден: {file_path}")
        return

    logger.info(f"✅ Файл: {file_path}")

    # Определяем дату
    price_date = None
    if args.date:
        try:
            price_date = datetime.strptime(args.date, '%Y-%m-%d').date()
            logger.info(f"📅 Дата из аргумента: {price_date}")
        except ValueError:
            logger.error("❌ Неверный формат даты. Используйте ГГГГ-ММ-ДД")
            return
    if price_date is None:
        price_date = parse_date_from_filename(args.file)
        if price_date: logger.info(f"📅 Дата из имени файла: {price_date}")
    if price_date is None:
        price_date = parse_date_from_excel(file_path)
        if price_date: logger.info(f"📅 Дата из Excel: {price_date}")
    if price_date is None:
        price_date = date.today()
        logger.warning(f"⚠️ Дата не найдена, используется сегодняшняя: {price_date}")

    conn = get_connection()

    def safe_conn():
        """Возвращает живое соединение, переподключаясь если нужно."""
        nonlocal conn
        if conn.closed:
            logger.info("♻️  Переподключение к БД...")
            conn = get_connection()
        return conn

    try:
        # ── Терем (STOUT / ROMMER, много листов) ──────────────────────────
        if args.type == 'terem':
            sheets_in_file = get_terem_sheets(file_path)
            logger.info(f"Листов STOUT/ROMMER в файле: {len(sheets_in_file)}")

            # Создаём все листы в БД до долгого парсинга
            sheet_ids, sheet_discounts = {}, {}
            for s in sheets_in_file:
                sid, disc = ensure_sheet_exists(conn, s, 0.0)
                sheet_ids[s]       = sid
                sheet_discounts[s] = disc
            conn.commit()

            # Парсим все листы (долгая операция — БД не трогаем)
            all_products = parse_terem_file(file_path, sheet_discounts, sheet_ids)

            # Каждый лист — отдельный коммит, защита от таймаута SSL
            total_stats = {'new': 0, 'changed': 0, 'unchanged': 0, 'total': 0}
            for sheet_name, products in all_products.items():
                if not products:
                    logger.warning(f"⚠️ {sheet_name}: товары не найдены")
                    continue
                try:
                    c = safe_conn()
                    st = save_products_to_db(c, products, price_date)
                    c.commit()
                    for k in total_stats:
                        total_stats[k] += st[k]
                    logger.info(f"  ✓ {sheet_name}: новых={st['new']} изм={st['changed']} без_изм={st['unchanged']}")
                except Exception as sheet_err:
                    logger.error(f"  ✗ {sheet_name}: {sheet_err}")
                    try:
                        conn.rollback()
                    except Exception:
                        pass

            logger.info(f"✅ Итого: {total_stats}")

        # ── Плоский прайс (один лист) ──────────────────────────────────────
        else:
            supplier  = args.supplier or os.path.splitext(args.file)[0]
            sheet_arg = args.sheet if args.sheet else 0
            sid, saved_disc = ensure_sheet_exists(conn, supplier, args.discount)
            actual_disc = args.discount if args.discount else saved_disc

            products = parse_flat_sheet(file_path, sheet_arg, sid, actual_disc)
            if not products:
                logger.warning("⚠️ Товары не найдены. Проверьте структуру файла.")
                return

            logger.info(f"✅ Найдено товаров: {len(products)}")
            stats = save_products_to_db(conn, products, price_date)
            conn.commit()
            logger.info(f"✅ Готово: {stats}")

    except Exception as e:
        logger.error(f"❌ Ошибка: {e}", exc_info=True)
        try:
            conn.rollback()
            logger.info("⚠️ Транзакция отменена")
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
        logger.info("Соединение закрыто")
        logger.info("=" * 50)


if __name__ == "__main__":
    main()