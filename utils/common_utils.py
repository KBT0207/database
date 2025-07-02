import pandas as pd
import re
import os
import pandas as pd
from logging_config import logger

def clean_text(value):
    if pd.isna(value):
        return ""
    value = str(value)
    value = re.sub(r'[\t\n\r]', '', value)
    value = re.sub(r'[^A-Za-z0-9]', '', value)
    value = value.strip()
    return value.title()


def read_file_safely(file_path):
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()
    excel_extensions = {'.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt'}

    try:
        if ext in excel_extensions:
            df = pd.read_excel(file_path)
        elif ext == '.csv':
            df = pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file extension: {ext}")
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to read file '{file_path}': {e}")



def calculate_qty(description, quantity):
    logger.info(f"Calculating quantity from description: {description}, quantity: {quantity}")
    
    pattern_kg = r'(\d+(?:\.\d+)?)\s*(?:KG|KGS)\b'
    pattern_other = r'(\d+)\s*(?:G|GM|GX|GMS|GC|GMN)?\s*(?:X|\s)\s*(\d+)\s*(?:PUNNET|MAP\s*BAGS)?'
    pattern_other1 = r'(\d+)[A-Z]+\s*(\d+)'
    pattern_box = r'\b(\d+)\s*B\s*X\s*(\d+)\s*GMS\s*X\s*(\d+)\s*PUNNET\b'
    pattern_non_numeric = r'^\D+$'
    
    try:
        quantity = float(quantity)
    except ValueError:
        return pd.Series([None, None, None, 'none'])

    number1, number2, number3 = None, None, None

    if re.match(pattern_non_numeric, description):
        return pd.Series([None, None, None, 'NO NUMBER'])

    match_box = re.search(pattern_box, description, re.IGNORECASE)
    if match_box:
        number1 = int(match_box.group(1))
        number2 = int(match_box.group(2))
        number3 = int(match_box.group(3))
        return pd.Series([number1, number2, number3, 'BOX'])

    match_other1 = re.search(pattern_other1, description, re.IGNORECASE)
    if match_other1:
        number1 = float(match_other1.group(1))
        number2 = float(match_other1.group(2))
        return pd.Series([number1, number2, None, 'OTHER'])

    match_kg = re.search(pattern_kg, description, re.IGNORECASE)
    if match_kg:
        number1 = float(match_kg.group(1))
        number2 = 1
        return pd.Series([number1, number2, None, 'KG'])

    match_other = re.search(pattern_other, description, re.IGNORECASE)
    if match_other:
        number1 = int(match_other.group(1))
        number2 = int(match_other.group(2))
        return pd.Series([number1, number2, None, 'OTHER'])

    return pd.Series([None, None, None, 'none'])
