"""
parsers.py — универсальный модуль парсинга прайс-листов.

Поддерживаемые форматы
───────────────────────
1. Терем (multi-sheet)
   Файл содержит десятки листов; парсим только те, чьё название содержит
   «STOUT» или «ROMMER» (регистронезависимо).
   Каждый лист → отдельная запись sheets в БД со своей скидкой.

   1a. Rommer СПР (Россия) — специфическая структура без колонки «Артикул»;
       код формируется из типа/высоты/длины.
   1b. Все остальные STOUT/ROMMER листы — универсальный парсер:
       ищет скидку в ячейке «Скидка», затем строку с «Артикул», далее данные.

2. Плоский прайс (flat, single-sheet)
   Один лист, заголовки в одной строке, есть колонки:
   «Код» (supplier code, опционально), «Артикул», «Наименование», «Цена».
   Скидка применяется к листу целиком, если задана.
   Примеры: Valfex, Галеев.

3. .xls файлы
   Требуют xlrd; при отсутствии библиотеки поднимают понятную ошибку.
"""

import re
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Паттерн артикула STOUT/ROMMER: XXX-0000-000000
_ARTICLE_RE = re.compile(r'^[A-Z]{2,5}[-_][A-Z0-9]{3,6}[-_][A-Z0-9]{4,12}\s*$')


def _is_stout_article(val) -> bool:
    if not isinstance(val, str):
        return False
    return bool(_ARTICLE_RE.match(val.strip()))


# ── Общие вспомогательные функции ────────────────────────────────────────────

def _find_discount(df: pd.DataFrame) -> float | None:
    for i in range(min(10, len(df))):
        for j in range(df.shape[1]):
            cell = df.iloc[i, j]
            if not isinstance(cell, str) or 'скидк' not in cell.lower():
                continue
            for k in range(j + 1, min(j + 6, df.shape[1])):
                try:
                    v = float(df.iloc[i, k])
                    if v == v:
                        return v
                except (ValueError, TypeError):
                    pass
            for k in range(i + 1, min(i + 3, len(df))):
                try:
                    v = float(df.iloc[k, j])
                    if v == v:
                        return v
                except (ValueError, TypeError):
                    pass
            return 0.0
    return None


def _find_header_row(df: pd.DataFrame, keywords=('артикул', 'код')) -> int | None:
    for i in range(min(20, len(df))):
        row_str = ' '.join(str(v).lower() for v in df.iloc[i] if str(v) != 'nan')
        if any(k in row_str for k in keywords):
            return i
    return None


def _find_col(df: pd.DataFrame, row: int, *keywords) -> int | None:
    for j in range(df.shape[1]):
        h = str(df.iloc[row, j]).lower()
        if any(k in h for k in keywords):
            return j
    return None


def _find_price_col(df: pd.DataFrame, header_row: int) -> int | None:
    candidates_rub, candidates_other = [], []
    # Строку +1 используем только для дополнения заголовка (двухстрочные шапки),
    # но проверку на скидку делаем только по основной строке заголовка.
    extra = [header_row + 1] if header_row + 1 < len(df) else []
    for j in range(df.shape[1]):
        main_h = str(df.iloc[header_row, j]).lower()
        full_h = main_h + ' ' + ' '.join(str(df.iloc[r, j]).lower() for r in extra)
        if 'цена' not in full_h and 'price' not in full_h:
            continue
        if 'скидк' in main_h or 'учет' in main_h:  # только основная строка
            continue
        (candidates_rub if 'руб' in full_h or '₽' in full_h else candidates_other).append(j)
    return (candidates_rub or candidates_other or [None])[0]


def _find_discounted_price_col(df: pd.DataFrame, header_row: int) -> int | None:
    rows = [header_row] + ([header_row + 1] if header_row + 1 < len(df) else [])
    for j in range(df.shape[1]):
        h = ' '.join(str(df.iloc[r, j]).lower() for r in rows)
        if ('цена' in h or 'price' in h) and ('скидк' in h or 'учет' in h):
            return j
    return None


def _find_name_col(df: pd.DataFrame, header_row: int, ref_col: int) -> int | None:
    j = _find_col(df, header_row, 'наименован', 'описани', 'название', 'тмц', 'модель')
    if j is not None:
        return j
    # Fallback: первый столбец правее ref_col с преимущественно текстом
    sample = range(header_row + 1, min(header_row + 16, len(df)))
    for j in range(ref_col + 1, min(ref_col + 5, df.shape[1])):
        texts = sum(
            1 for k in sample
            if not pd.isna(df.iloc[k, j])
            and not _try_float(df.iloc[k, j])
            and isinstance(df.iloc[k, j], str)
            and len(df.iloc[k, j].strip()) > 4
        )
        if texts >= 3:
            return j
    return None


def _try_float(val) -> bool:
    try:
        float(val)
        return True
    except (ValueError, TypeError):
        return False


def _safe_float(val) -> float | None:
    try:
        v = float(val)
        return v if v == v and v > 0 else None
    except (ValueError, TypeError):
        return None


def _safe_str(val) -> str:
    if pd.isna(val):
        return ''
    s = str(val).strip()
    return '' if s.lower() == 'nan' else s


# ════════════════════════════════════════════════════════════════════════════
# Парсер 1a — Rommer СПР (панельные радиаторы, нет колонки «Артикул»)
# ════════════════════════════════════════════════════════════════════════════

def parse_rommer_spr(
    file_path: str, sheet_name: str, sheet_id: int, discount_percent: float
) -> list[dict]:
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    logger.info(f"[Rommer СПР] {df.shape}")
    products = []
    current_type = current_height = None
    i = 6
    while i < len(df):
        row = df.iloc[i]
        col_0 = str(row[0]) if pd.notna(row[0]) else ''
        if 'тип' in col_0.lower():
            m = re.search(r'(\d{2})', col_0)
            if m:
                current_type = m.group(1)
            i += 1
            continue
        if current_type:
            h = row[1] if pd.notna(row[1]) else None
            l = row[2] if pd.notna(row[2]) else None
            if not h and not l:
                i += 1; continue
            if isinstance(h, str) and not h.isdigit():
                i += 1; continue
            if pd.notna(row[1]) and isinstance(row[1], (int, float)):
                current_height = int(row[1])
            if current_height is None or l is None:
                i += 1; continue
            try:
                hv, lv = int(current_height), int(l)
            except (ValueError, TypeError):
                i += 1; continue
            ps = _safe_float(row[7] if pd.notna(row[7]) else None)
            pb = _safe_float(row[8] if pd.notna(row[8]) else None)
            hc = str(hv)[0]
            lc = str(lv // 10).zfill(3)
            if ps:
                products.append({
                    'sheet_id': sheet_id, 'article': f'RRS-2010-{current_type}{hc}{lc}',
                    'code': None,
                    'name': f'ROMMER Радиатор стальной {current_type}/{hv}/{lv} боковое подключение Compact',
                    'price_retail': ps, 'discount_percent': discount_percent,
                })
            if pb:
                products.append({
                    'sheet_id': sheet_id, 'article': f'RRS-2020-{current_type}{hc}{lc}',
                    'code': None,
                    'name': f'ROMMER Радиатор стальной {current_type}/{hv}/{lv} нижнее подключение Ventil Compact',
                    'price_retail': pb, 'discount_percent': discount_percent,
                })
        i += 1
    logger.info(f"[Rommer СПР] Найдено: {len(products)}")
    return products


# ════════════════════════════════════════════════════════════════════════════
# Парсер 1b — универсальный для листов Терема (STOUT/ROMMER)
# ════════════════════════════════════════════════════════════════════════════

def parse_terem_sheet(
    file_path: str, sheet_name: str, sheet_id: int,
    discount_percent: float | None = None,
) -> list[dict]:
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    logger.info(f"[Terem/{sheet_name}] {df.shape}")
    if discount_percent is None:
        discount_percent = _find_discount(df) or 0.0
    header_row = _find_header_row(df)
    price_col  = _find_price_col(df, header_row) if header_row is not None else None
    products, seen, name_col = [], set(), None
    start = (header_row + 1) if header_row is not None else 0
    for i in range(start, len(df)):
        row = df.iloc[i]
        article, art_col = None, None
        for j in range(min(4, df.shape[1])):
            v = _safe_str(row[j])
            if _is_stout_article(v):
                article, art_col = v, j; break
        if not article or article in seen:
            continue
        seen.add(article)
        if name_col is None and header_row is not None:
            name_col = _find_name_col(df, header_row, art_col)
        price = _safe_float(row[price_col]) if price_col is not None else None
        if price is None:
            for j in range(art_col + 1, len(row)):
                v = _safe_float(row[j])
                if v and v > 10:
                    price = v; break
        if not price:
            continue
        name = _safe_str(row[name_col]) if name_col is not None else ''
        products.append({
            'sheet_id': sheet_id, 'article': article, 'code': None,
            'name': (name or f'{sheet_name} {article}')[:500],
            'price_retail': round(price, 2), 'discount_percent': discount_percent,
        })
    logger.info(f"[Terem/{sheet_name}] Найдено: {len(products)}")
    return products


# ════════════════════════════════════════════════════════════════════════════
# Парсер 2 — плоский прайс (Valfex, Галеев и любые одно-листовые)
# ════════════════════════════════════════════════════════════════════════════

def parse_flat_sheet(
    file_path: str, sheet_name: str | int, sheet_id: int,
    discount_percent: float = 0.0,
) -> list[dict]:
    """
    Плоский прайс. Поддерживает:
    - Колонки «Код» (supplier code) + «Артикул» (producer article)
    - Только «Артикул» / только «Код»
    - Готовую колонку «Цена со скидкой» или расчёт по переданной скидке
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    except ImportError as e:
        raise RuntimeError(
            f"Для чтения .xls нужен xlrd. Добавьте xlrd в requirements.txt. ({e})"
        )
    logger.info(f"[Flat/{sheet_name}] {df.shape}")

    # Скидка из файла (если не передана)
    file_disc = _find_discount(df)
    if discount_percent == 0.0 and file_disc is not None:
        discount_percent = file_disc

    header_row = _find_header_row(df, keywords=('артикул', 'код', 'наименован'))
    if header_row is None:
        logger.warning(f"[Flat/{sheet_name}] Заголовки не найдены"); return []

    article_col    = _find_col(df, header_row, 'артикул')
    # Ищем «Код» — отдельная колонка, не часть «Артикул»
    code_col = None
    for j in range(df.shape[1]):
        h = str(df.iloc[header_row, j]).lower().strip()
        if h in ('код', 'code', 'код товара', 'код поставщика'):
            code_col = j
            break
    # fallback — если артикул не нашли, ищем шире
    if article_col is None and code_col is None:
        code_col = _find_col(df, header_row, 'код')
    use_code_as_article = article_col is None and code_col is not None
    ref_col = article_col if article_col is not None else (code_col or 0)
    name_col       = _find_name_col(df, header_row, ref_col)
    price_col      = _find_price_col(df, header_row)
    disc_price_col = _find_discounted_price_col(df, header_row)

    logger.info(
        f"[Flat/{sheet_name}] article={article_col} code={code_col} "
        f"name={name_col} price={price_col} disc_price={disc_price_col} "
        f"discount={discount_percent}%"
    )

    products, seen = [], set()
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]
        if use_code_as_article:
            article = _safe_str(row[code_col])
            code    = None
        else:
            article = _safe_str(row[article_col]) if article_col is not None else ''
            code    = _safe_str(row[code_col]) if code_col is not None else None

        if not article or article in seen:
            continue

        price = _safe_float(row[price_col]) if price_col is not None else None
        if price is None:
            # Fallback: последнее число в строке
            for j in range(len(row) - 1, max(-1, len(row) - 8), -1):
                v = _safe_float(row[j])
                if v and v > 0:
                    price = v; break
        if not price:
            continue

        seen.add(article)

        # Цена со скидкой
        price_discounted = None
        if disc_price_col is not None:
            price_discounted = _safe_float(row[disc_price_col])
        if price_discounted is None and discount_percent:
            price_discounted = round(price * (1 - discount_percent / 100), 2)

        name = _safe_str(row[name_col]) if name_col is not None else ''

        products.append({
            'sheet_id': sheet_id,
            'article': article[:100],
            'code': (code[:100] if code else None),
            'name': (name or article)[:500],
            'price_retail': round(price, 2),
            'price_discounted': price_discounted,
            'discount_percent': discount_percent,
        })

    logger.info(f"[Flat/{sheet_name}] Найдено: {len(products)}")
    return products


# ════════════════════════════════════════════════════════════════════════════
# Роутинг
# ════════════════════════════════════════════════════════════════════════════

def get_terem_sheets(file_path: str) -> list[str]:
    """Листы Терема, которые нужно парсить (STOUT или ROMMER, любой регистр)."""
    xl = pd.ExcelFile(file_path)
    return [
        s for s in xl.sheet_names
        if 'stout' in s.lower() or 'rommer' in s.lower()
    ]


def parse_terem_file(
    file_path: str,
    sheet_discounts: dict[str, float],
    sheet_ids: dict[str, int],
) -> dict[str, list[dict]]:
    """
    Парсит все STOUT/ROMMER листы из файла Терема.
    sheet_discounts / sheet_ids берутся из БД (таблица sheets).
    """
    result = {}
    for sheet in get_terem_sheets(file_path):
        sid  = sheet_ids.get(sheet)
        disc = sheet_discounts.get(sheet, 0.0)
        if sid is None:
            logger.warning(f"[Terem] Лист «{sheet}» не в БД, пропускаем"); continue
        if sheet.lower() == 'rommer спр (россия)':
            result[sheet] = parse_rommer_spr(file_path, sheet, sid, disc)
        else:
            result[sheet] = parse_terem_sheet(file_path, sheet, sid, disc)
    return result


def parse_single_sheet(
    file_path: str,
    sheet_name: str | int,
    sheet_id: int,
    discount_percent: float = 0.0,
    supplier_type: str = 'flat',
) -> list[dict]:
    """
    Точка входа для app.py — парсит один лист.
    supplier_type: 'flat' (по умолчанию) или добавь новые ключи при необходимости.
    """
    if supplier_type == 'terem':
        raise ValueError("Для Терема используй parse_terem_file()")
    return parse_flat_sheet(file_path, sheet_name, sheet_id, discount_percent)