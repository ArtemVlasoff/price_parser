"""
utils.py — общие вспомогательные функции.
"""
import os
import re
import logging
from datetime import datetime, date
import pandas as pd

logger = logging.getLogger(__name__)


def parse_date_from_filename(filename: str) -> date | None:
    """
    Извлекает дату из имени файла.
    Форматы: Терем__2026-03-18.xlsx, YYYY-MM-DD, YYYY_MM_DD, YYYYMMDD
    """
    base = os.path.splitext(filename)[0]

    # Терем__2026-03-18
    m = re.search(r'__(\d{4}[-_]\d{2}[-_]\d{2})', base)
    if m:
        try:
            return datetime.strptime(m.group(1).replace('_', '-'), '%Y-%m-%d').date()
        except ValueError:
            pass

    # YYYY-MM-DD или YYYY_MM_DD
    m = re.search(r'(\d{4})[-_](\d{2})[-_](\d{2})', base)
    if m:
        try:
            return datetime(*map(int, m.groups())).date()
        except ValueError:
            pass

    # YYYYMMDD
    m = re.search(r'(\d{8})', base)
    if m:
        try:
            return datetime.strptime(m.group(1), '%Y%m%d').date()
        except ValueError:
            pass

    return None


def parse_date_from_excel(file_path: str) -> date | None:
    """Извлекает дату из ячейки A1 первого листа Excel."""
    try:
        df = pd.read_excel(file_path, sheet_name=0, header=None, nrows=1, usecols=[0])
        val = df.iloc[0, 0]
        if pd.isna(val):
            return None
        if isinstance(val, (datetime, pd.Timestamp)):
            return val.date()
        if isinstance(val, str):
            for fmt in ['%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y']:
                try:
                    return datetime.strptime(val.strip(), fmt).date()
                except ValueError:
                    continue
    except Exception as e:
        logger.debug(f"Не удалось прочитать дату из Excel: {e}")
    return None