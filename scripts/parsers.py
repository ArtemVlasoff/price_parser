import pandas as pd
import re
import logging

logger = logging.getLogger(__name__)

def parse_rommer_sheet(file_path, sheet_name, sheet_id, discount_percent):
    """Парсер для листов Rommer СПР"""
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    products = []
    current_type = None
    current_height = None
    
    logger.info(f"Парсинг листа '{sheet_name}' (размер: {df.shape})")
    
    # Начинаем со строки 6 (после заголовков)
    i = 6
    
    while i < len(df):
        row = df.iloc[i]
        
        # Получаем значение в колонке 0
        col_0 = str(row[0]) if pd.notna(row[0]) else ""
        
        # Проверяем, это строка типа (11 тип, 21 тип, и т.д.)
        if "тип" in col_0.lower():
            # Извлекаем номер типа (11, 21, 22, 33)
            match = re.search(r'(\d{2})', col_0)
            if match:
                current_type = match.group(1)
                logger.debug(f"Найден тип: {current_type} на строке {i}")
            i += 1
            continue
        
        # Если текущий тип задан, парсим данные
        if current_type:
            # Высота (col 1)
            height = row[1] if pd.notna(row[1]) else None
            
            # Длина (col 2)
            length = row[2] if pd.notna(row[2]) else None
            
            # Если нет высоты и длины, пропускаем
            if not height and not length:
                i += 1
                continue
            
            # Пропускаем строки-заголовки (если col[1] содержит текст типа "Кол-во", "Высота" и т.д.)
            if isinstance(height, str) and not height.isdigit():
                i += 1
                continue
            
            # Если есть высота, сохраняем её
            if pd.notna(row[1]) and isinstance(row[1], (int, float)):
                current_height = int(row[1])
            
            # Используем текущую высоту и текущую длину
            if current_height is None or length is None:
                i += 1
                continue
            
            try:
                height_val = int(current_height)
                length_val = int(length)
            except (ValueError, TypeError):
                i += 1
                continue
            
            # Цены
            price_side = row[7] if pd.notna(row[7]) else None     # Боковое подключение
            price_bottom = row[8] if pd.notna(row[8]) else None   # Нижнее подключение
            
            # Если обе цены пусто, пропускаем
            if price_side is None and price_bottom is None:
                i += 1
                continue
            
            try:
                price_side = float(price_side) if price_side else None
                price_bottom = float(price_bottom) if price_bottom else None
            except (ValueError, TypeError):
                i += 1
                continue
            
            # Формируем коды для артикула
            height_code = str(height_val)[0]
            length_code = str(length_val).zfill(3)
            
            # Боковое подключение
            if price_side:
                article_side = f"RRS-2010-{current_type}{height_code}{length_code}"
                products.append({
                    'sheet_id': sheet_id,
                    'article': article_side,
                    'name': f"ROMMER Радиатор стальной {current_type}/500/{length_val} боковое подключение Compact",
                    'price_retail': price_side,
                    'discount_percent': discount_percent
                })
            
            # Нижнее подключение
            if price_bottom:
                article_bottom = f"RRS-2020-{current_type}{height_code}{length_code}"
                products.append({
                    'sheet_id': sheet_id,
                    'article': article_bottom,
                    'name': f"ROMMER Радиатор стальной {current_type}/500/{length_val} нижнее подключение Ventil Compact",
                    'price_retail': price_bottom,
                    'discount_percent': discount_percent
                })
        
        i += 1
    
    logger.info(f"Всего найдено товаров: {len(products)}")
    return products