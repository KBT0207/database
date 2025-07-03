import os
import re
import glob
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Union, Optional
from sqlalchemy import delete, func
from sqlalchemy.exc import SQLAlchemyError
import pycountry_convert as pc

from utils.common_utils import (
    read_file_safely,
    calculate_qty,
    clean_text,
    parse_date_flexibly
)
from sql_connector import kbbio_engine, kbe_engine, kbbio_connector, kbe_connector
from models.base import KBEBase, KBBIOBase
from models.kbe.kbe_models import KBEImportExport, KBEImportExportMapping
from models.shiprocket.shiprocket_models import ShiprocketOrder
from dbcrud import DatabaseCrud
from Shiprocket.shiprocket import get_all_orders
from logging_config import logger
from sqlalchemy.orm import sessionmaker



def clean_to_the_order(name: str) -> str:
    if pd.isna(name):
        return 'TO ORDER'

    name = str(name).upper()

    name = re.sub(r'[^A-Z0-9 ]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()

    name = re.sub(r'^(Z\s+)?TO\s+', 'TO ', name)
    name = re.sub(r'\bN\s*A\b', '', name)
    name = re.sub(r'\bNA\b', '', name)

    name = re.sub(r'\b(OREDER|ORDDER|OTDER|ORDFER|ORER|OEDER|ORDR|ORDE|OREDR|OREDRR)\b', 'ORDER', name)

    if re.fullmatch(r'TO\s+(THE\s+)?ORDER', name):
        return 'TO ORDER'

    if name.startswith("TO THE ORDER OF"):
        entity = name.replace("TO THE ORDER OF", "").strip()
        if not entity or re.fullmatch(r'[.\-/\\ ]*', entity):
            return 'TO ORDER'
        return f'TO THE ORDER OF {entity}'

    if name.startswith("TO ORDER OF"):
        entity = name.replace("TO ORDER OF", "").strip()
        if not entity or re.fullmatch(r'[.\-/\\ ]*', entity):
            return 'TO ORDER'
        return f'TO ORDER OF {entity}'

    if name.startswith("TO THE ORDER"):
        return "TO THE ORDER"

    if name.startswith("TO ORDER"):
        return "TO ORDER"

    return name

def get_continent(country_name):
    try:
        country_code = pc.country_name_to_country_alpha2(country_name, cn_name_format="default")
        continent_code = pc.country_alpha2_to_continent_code(country_code)
        return pc.convert_continent_code_to_continent_name(continent_code)
    except:
        return 'Unknown'


def custom_data_processor(file_path: str) -> pd.DataFrame:
    """
    Reads and processes a supported data file (Excel or CSV) into a cleaned pandas DataFrame.

    Parameters:
        file_path (str): Path to the input file. Supported formats: 
                        .xlsx, .xls, .xlsm, .xlsb, .ods, .odf, .odt, .csv

    Returns:
        pd.DataFrame: A cleaned and preprocessed DataFrame ready for analysis or database import.
    """
    ...


    warnings.filterwarnings("ignore", message="This pattern is interpreted as a regular expression.*")
    df = read_file_safely(file_path=file_path)
    df.columns = df.columns.str.lower().str.replace(" ","_")
    df['product_classified'] = pd.Series([pd.NA] * len(df), dtype='string')
    df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y', errors='coerce')


    num_col = [
        'quantity','fob_value_inr','unit_price_inr',
        'fob_value_usd','fob_value_foreign_currency',
        'unit_price_foreign_currency','fob_value_in_lacs_inr','item_no',
        'drawback','chapter','hs_4_digit','hs_code','pin_code','year'
        ]
    for col in num_col:
        if col in df.columns:
            df[col] = (df[col].astype(str).str.replace(r'[^\d\.]', '', regex=True).replace('', None))
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    if 'pin_code' in df.columns:
        df['pin_code'] = (
            df['pin_code']
            .astype(str)
            .str.replace(r'[^\d]', '', regex=True)
            .replace('', None)
            .astype(float)
            .astype('Int64')
        )

    other_col = [col for col in df.columns if col not in num_col and col != 'date']

    for col in other_col:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(r'\t+', '', regex=True)
                .str.encode('ascii', 'ignore').str.decode('ascii')
        
            )

    classification_rules = [
        {
            'label': 'COCONUT',
            'include': r'\bcoconut\b',
            'exclude': r'\b(slice|frozen|cut|dry|dried|powder|milk|chunk|juice|ice)\b',
        },
        {
            'label': 'FRESH GARLIC',
            'include': r'\bgarlic\b',
            'exclude': r'\b(slice|frozen|cut|dry|dried|powder|ice)\b',
        },
        {
            'label': 'MIX FRUITS & VEG',
            'include': r'\b(mixed fruits|mixed vegetables|mix fruit|mix vegetables|mix vegitable|mixed vegetables)\b',
            'exclude': r'\b(slice|frozen|cut|dry|dried|powder|milk|chunk|juice|ice)\b',
        },
        {
            'label': 'DRUMSTICK',
            'include': r'\bdrumsticks?\b',
            'exclude': r'\b(slice|frozen|cut|dry|dried|powder|milk|chunk|juice|ice)\b',
        },
        {
            'label': 'DRAGON FRUITS',
            'include': r'\bdragon\b',
            'exclude': r'\b(slice|frozen|cut|dry|dried|powder|milk|chunk|juice|ice)\b',
        },
        {
            'label': 'MANGO',
            'include': r'\b(mango|alphanso)\b',
            'exclude': r'\b(pulp|slice|frozen|cut mango|pickle|papad|cut|dry|dried|powder|raw|juice)\b',
        },
        {
            'label': 'BABY CORN',
            'include': r'\b(baby|babycorn|baby corn)\b',
            'exclude': r'\b(okra|potato|frozen|iqf|cut|brine|acetic|acid|onion|bananas?|wheat|babyvita|bitter)\b',
        },
        {
            'label': 'POMEGRANATES',
            'include': r'\b(pome|anar|pomegranate)\b',
            'exclude': r'\b(aril|pulp|arils|dhana|dana|frozen|iqf|cut|brine)\b',
        },
        {
            'label': 'POMEGRANATES ARILS',
            'include': r'\b(aril|pomegranate arils|arils)\b',
            'exclude': r'\b(vinegar|frozen|iqf|cut|brine|acetic|acid|dry|dried)\b',
        },
        {
            'label': 'OKRA',
            'include': r'\b(okra|lady finger)\b',
            'exclude': r'\b(frozen|iqf|cut|brine|acetic|acid|dry|dried)\b',
        },
        {
            'label': 'CHILLI',
            'include': r'\b(chilli|chilly)\b',
            'exclude': r'\b(frozen|iqf|cut|brine|acetic|acid|dry|dried)\b',
        },
        {
            'label': 'GUAVA',
            'include': r'\b(guava|peru)\b',
            'exclude': r'\b(pulp|iqf|cut|brine|acetic|acid|dry|dried)\b',
        },
        {
            'label': 'CHICKOO',
            'include': r'\b(sapota|chickoo)\b',
            'exclude': r'\b(pulp|slice|iqf|cut|brine|acetic|acid|dry|dried|frozen)\b',
        },
        {
            'label': 'DUDHI',
            'include': r'\b(dudhi|bottle gourd|bottleguard)\b',
            'exclude': r'\b(pulp|slice|iqf|cut|brine|acetic|acid|dry|dried|frozen)\b',
        },
        {
            'label': 'ONION',
            'include': r'\b(red onion|shallot|small onion|onion)\b',
            'exclude': r'\b(iqf|cut|brine|acetic|acid|dry|dried|frozen)\b',
        },
    ]

    for rule in classification_rules:
        include = df['product_description'].str.contains(rule['include'], case=False, na=False)
        exclude = df['product_description'].str.contains(rule['exclude'], case=False, na=False)
        df.loc[include & ~exclude, 'product_classified'] = rule['label']

    exporter_patterns = {
        r'\bKAY\sBEE\b': 'KAY BEE EXPORTS',
        r'\bMAGNUS\b': 'MAGNUS FARM',
        r'\bFRESHTROP FRUITS\b': 'FRESHTROP FRUITS',
        r'\bGREEN AGREVOLUTION\b': 'GREEN AGREVOLUTION',
        r'\bBARAMATI\b': 'BARAMATI AGRO',
        r'\bULINK AGRITECH\b': 'ULINK AGRITECH',
        r'\bSAM AGRI FRESH\b': 'SAM AGRI FRESH',
        r'\bSANTOSH EXPORTS\b': 'SANTOSH EXPORTS',
        r'\bKASHI EXPORTS\b': 'KASHI EXPORTS',
        r'\bKHUSHI INTERNATIONAL\b': 'KHUSHI INTERNATIONAL',
        r'\bGO GREEN\b': 'GO GREEN EXPORT',
        r'\bTHREE CIRCLES\b': 'THREE CIRCLES',
        r'\bALL SEASON\b': 'ALL SEASON EXPORTS',
        r'\bM\.?\s*K\.?\s*EXPORTS\b': 'M.K. EXPORTS',
        r'\bESSAR IMPEX\b': 'ESSAR IMPEX',
        r'\bESSAR EXPORTS\b': 'ESSAR EXPORTS',
        r'\bSUPER FRESH FRUITS\b': 'SUPER FRESH FRUIT',
        r'\bVASHINI EXPORTS\b': 'VASHINI EXPORTS',
        r'\bSCION AGRICOS\b': 'SCION AGRICOS',
        r'\bMANTRA INTERNATIONAL\b': 'MANTRA INTERNATIONAL',
        r'\bSIA IMPEX\b': 'SIA IMPEX',
    }

    for pattern, name in exporter_patterns.items():
        df.loc[
            df['indian_exporter_name'].str.contains(pattern, case=False, na=False, regex=True), 
            'indian_exporter_name'
        ] = name

    foreign_importer_patterns = {
        r'\bWealmoor|Weal Moor\b': 'WEAL MOOR LTD',
        r'\bFLAMINGO|FLAMINGO PRODUCE\b': 'FLAMINGO PRODUCE',
        r'\bMINOR|MINOR, WEIR AND WILLIS LIMITED|MINOR WEIR & WILLIS LIMITED\b': 'MINOR WEIR & WILLIS LIMITED',
        r"\bNATURE'S PRIDE\b": "NATURE'S PRIDE",
        r'\bYUKON\b': 'YUKON INTERNATION',
        r'\bJALARAM PRODUCE\b': 'JALARAM PRODUCE',
        r'\bRAJA FOODS & VEGETABLE\b': 'RAJA FOODS & VEGETABLES',
        r'\bDPS\b': 'DPS',
        r'\bBARAKAT\b': 'BARAKAT VEGETABLE',
        r'\bBARFOOTS\b': 'BARFOOTS OF BOTELY',
        r'\bCORFRESH|COREFRESH\b': 'COREFRESH LTD',
        r'\bPROVENANCE PARTNERS\b': 'PROVENANCE PARTNERS',
        r'\bS & F GLOBAL|S&F GLOBAL\b': 'S & F GLOBAL FRESH',
        r'\bBERRYMOUNT VEGETABLES\b': 'BERRYMOUNT VEGETABLES',
    }

    for pattern, standard in foreign_importer_patterns.items():
        df.loc[
            df['foreign_importer_name'].str.contains(pattern, case=False, na=False, regex=True),
            'foreign_importer_name'
        ] = standard

    for col in ['foreign_importer_name', 'indian_exporter_name']:
        pattern = re.compile(r'^[\s\.,]*$')
        df.loc[df[col].str.match(pattern, na=False), col] = 'TO ORDER'
        df[col] = df[col].astype(str).str.strip(' \t\n\r",.*&')
        df[col] = df[col].apply(clean_to_the_order)

    df['region'] = (
        df['foreign_country']
        .astype(str)
        .str.upper()
        .str.replace(r'[^A-Z ]', '', regex=True)
        .str.strip()
        .str.title()
        .apply(get_continent)
    )

    conversion_map = {
        'KGA': 1,
        'KGS': 1,
        'LBS': 0.453592,
        'QTL': 100,
        'MTS': 1000,
        'MTR':1000,
    }

    final_col = [
    'date', 'hs_code', 'product_description', 'product_classified','quantity', 'unit',
    'fob_value_inr', 'unit_price_inr', 'fob_value_usd', 'fob_value_foreign_currency',
    'unit_price_foreign_currency', 'currency_name', 'fob_value_in_lacs_inr',
    'iec', 'indian_exporter_name', 'exporter_address', 'exporter_city',
    'pin_code', 'cha_name', 'foreign_importer_name', 'importer_address',
    'importer_country', 'foreign_port', 'foreign_country', 'indian_port',
    'item_no', 'drawback', 'chapter', 'hs_4_digit', 'month', 'year','region']

    if 'importer_country' not in df.columns:
        df['importer_country'] = df['foreign_country']
        logger.info("Added column 'importer_country' from 'foreign_country'.")

    for col in final_col:
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found in input. Creating with default values.")
            if col in num_col:
                df[col] = 0
            elif col == 'date':
                df[col] = pd.NaT
            elif col == 'product_classified':
                df[col] = 'UNCLASSIFIED'
            elif col == 'region':
                df[col] = 'Unknown'
            else:
                df[col] = ''


    df = df[final_col]

    return df



def kbe_custom_import_export(file: str, custom_data: Optional[bool] = None, mapping_importer: Optional[bool] = None):
    KBEBase.metadata.create_all(bind=kbe_engine)
    db_kbe = DatabaseCrud(kbe_connector)

    if custom_data is True:
        custom_df = custom_data_processor(file_path=file)

        if custom_df.empty:
            logger.warning("No data to import from custom_data_processor.")
            return

        custom_df.columns = custom_df.columns.str.lower().str.strip()

        if 'date' not in custom_df.columns:
            logger.error("'date' column is missing in input file.")
            return

        custom_df['date'] = pd.to_datetime(custom_df['date'], errors='coerce')
        custom_df = custom_df.dropna(subset=['date'])

        start_date = custom_df['date'].min().date().isoformat()
        end_date = custom_df['date'].max().date().isoformat()

        try:
            db_kbe.delete_date_range_query(
                table_name='kbe_import_export',
                start_date=start_date,
                end_date=end_date,
                commit=True
            )

            db_kbe.import_data(
                table_name='kbe_import_export',
                df=custom_df,
                commit=True
            )
            logger.info(f"Imported custom data from {start_date} to {end_date} into 'kbe_import_export'.")

        except SQLAlchemyError as e:
            logger.exception("Error occurred during custom data import:")

    elif mapping_importer is True:
        try:
            df = pd.read_excel(file)
            df.columns = df.columns.str.lower().str.replace(" ", "_")

            if not df.empty:
                final_col = ['original_importer_name', 'standardized_importer_name']
                df['standardized_importer_name'] = df['standardized_importer_name'].apply(clean_text)
                df = df[final_col]

                db_kbe.import_data(table_name='kbe_importer_mapping', df=df, commit=True)
                logger.info(f"Imported {len(df)} records into 'kbe_importer_mapping'.")
            else:
                logger.warning("No data to import from item_mapping_import.")
        except Exception as e:
            logger.exception("Error occurred during importer mapping import:")

    else:
        logger.warning("No import option selected.")




SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=kbe_engine)

def product_classification():
    classification_rules = [
        {
            'label': 'COCONUT',
            'include': r'\bcoconut\b',
            'exclude': r'\b(slice|frozen|cut|dry|dried|powder|milk|chunk|juice|ice)\b',
        },
        {
            'label': 'FRESH GARLIC',
            'include': r'\bgarlic\b',
            'exclude': r'\b(slice|frozen|cut|dry|dried|powder|ice)\b',
        },
        {
            'label': 'MIX FRUITS & VEG',
            'include': r'\b(mixed fruits|mixed vegetables|mix fruit|mix vegetables|mix vegitable|mixed vegetables)\b',
            'exclude': r'\b(slice|frozen|cut|dry|dried|powder|milk|chunk|juice|ice)\b',
        },
        {
            'label': 'DRUMSTICK',
            'include': r'\bdrumsticks?\b',
            'exclude': r'\b(slice|frozen|cut|dry|dried|powder|milk|chunk|juice|ice)\b',
        },
        {
            'label': 'DRAGON FRUITS',
            'include': r'\bdragon\b',
            'exclude': r'\b(slice|frozen|cut|dry|dried|powder|milk|chunk|juice|ice)\b',
        },
        {
            'label': 'MANGO',
            'include': r'\b(mango|alphanso)\b',
            'exclude': r'\b(pulp|slice|frozen|cut mango|pickle|papad|cut|dry|dried|powder|raw|juice)\b',
        },
        {
            'label': 'BABY CORN',
            'include': r'\b(baby|babycorn|baby corn)\b',
            'exclude': r'\b(okra|potato|frozen|iqf|cut|brine|acetic|acid|onion|bananas?|wheat|babyvita|bitter)\b',
        },
        {
            'label': 'POMEGRANATES',
            'include': r'\b(pome|anar|pomegranate)\b',
            'exclude': r'\b(aril|pulp|arils|dhana|dana|frozen|iqf|cut|brine)\b',
        },
        {
            'label': 'POMEGRANATES ARILS',
            'include': r'\b(aril|pomegranate arils|arils)\b',
            'exclude': r'\b(vinegar|frozen|iqf|cut|brine|acetic|acid|dry|dried)\b',
        },
        {
            'label': 'OKRA',
            'include': r'\b(okra|lady finger)\b',
            'exclude': r'\b(frozen|iqf|cut|brine|acetic|acid|dry|dried)\b',
        },
        {
            'label': 'CHILLI',
            'include': r'\b(chilli|chilly)\b',
            'exclude': r'\b(frozen|iqf|cut|brine|acetic|acid|dry|dried)\b',
        },
        {
            'label': 'GUAVA',
            'include': r'\b(guava|peru)\b',
            'exclude': r'\b(pulp|iqf|cut|brine|acetic|acid|dry|dried)\b',
        },
        {
            'label': 'CHICKOO',
            'include': r'\b(sapota|chickoo)\b',
            'exclude': r'\b(pulp|slice|iqf|cut|brine|acetic|acid|dry|dried|frozen)\b',
        },
        {
            'label': 'DUDHI',
            'include': r'\b(dudhi|bottle gourd|bottleguard)\b',
            'exclude': r'\b(pulp|slice|iqf|cut|brine|acetic|acid|dry|dried|frozen)\b',
        },
        {
            'label': 'ONION',
            'include': r'\b(red onion|shallot|small onion|onion)\b',
            'exclude': r'\b(iqf|cut|brine|acetic|acid|dry|dried|frozen)\b',
        },
    ]

    session = SessionLocal()
    try:
        products = session.query(KBEImportExport).all()
        for product in products:
            desc = (product.product_description or "").lower()

            matched_label = None
            for rule in classification_rules:
                if re.search(rule['include'], desc):
                    if not re.search(rule['exclude'], desc):
                        matched_label = rule['label']
                        break

            product.product_classified = matched_label or "UNCLASSIFIED"

        session.commit()
        print("✅ Product classification completed successfully.")
    except SQLAlchemyError as e:
        session.rollback()
        print(f"❌ Error: {e}")
    finally:
        session.close()
