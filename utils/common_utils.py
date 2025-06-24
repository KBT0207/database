import pandas as pd
import re

def clean_text(value):
    if pd.isna(value):
        return ""
    # Remove tabs, newlines, extra spaces, and special characters
    value = str(value)
    value = re.sub(r'[\t\n\r]', '', value)        # Remove tabs and newlines
    value = re.sub(r'[^A-Za-z0-9]', '', value)    # Remove all special characters
    value = value.strip()                         # Remove leading/trailing whitespace
    return value.upper()  # or .lower() if you prefer


