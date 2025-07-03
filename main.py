from models.base import KBEBase, KBBIOBase
from models.kbe.kbe_models import KBEImportExport, KBEImportExportMapping
from models.shiprocket.shiprocket_models import ShiprocketOrder
import pandas as pd
from sql_connector import  kbbio_engine, kbe_engine, kbbio_connector, kbe_connector
from dbcrud import DatabaseCrud
from Shiprocket.shiprocket import get_all_orders
from datetime import datetime, date, timedelta
from logging_config import logger
from kbexports.kbe_processor import custom_data_processor
from typing import Optional
from utils.common_utils import clean_text
from sqlalchemy import delete, func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
import glob
import os
from kbexports.kbe_processor import kbe_custom_import_export,product_classification
import re
import platform
from itertools import chain


def shiprocket_daily(from_days:int):
    KBBIOBase.metadata.create_all(bind=kbbio_engine)
    db_kbbio = DatabaseCrud(kbbio_connector)
    from_date = datetime.today() - timedelta(days=from_days)
    to_date = datetime.today()
    from_date_str = from_date.strftime('%Y-%m-%d')
    to_date_str = to_date.strftime('%Y-%m-%d')
    try:
        df = get_all_orders(start_date=from_date_str, end_date=to_date_str)
        if df.empty:
            logger.info("No orders found to process.")
            return
        
        shiprocket_ids = df['shiprocket_id'].dropna().unique().tolist()

        if shiprocket_ids:
            db_kbbio.delete_shiprocket_id_wise(shiprocket_ids,commit=True)
        else:
            logger.info("No Shiprocket IDs found in data. Skipping deletion step.")
        
        db_kbbio.import_data(table_name='shiprocket_orders',df=df,commit=True)

    except Exception as e:
        logger.error("Shiprocket sync failed", exc_info=True)


def folder_path_wise_custom_data_import_in_db(path: str):
    current_os = platform.system()

    if current_os == "Linux" and path[1:3] == ":\\":
        drive_letter = path[0].lower()
        linux_path = "/mnt/" + drive_letter + path[2:].replace("\\", "/")
        path = linux_path

    path = os.path.normpath(path)

    all_files = glob.glob(os.path.join(path, "**", "*.*"), recursive=True)
    valid_files = [f for f in all_files if f.lower().endswith((".xlsx", ".csv"))]

    if not valid_files:
        logger.info(f"[INFO] No Excel or CSV files found in path: {path}")
        return

    for file in valid_files:
        logger.info(f"[PROCESSING] {file}")
        try:
            kbe_custom_import_export(file=file, custom_data=True)
        except Exception as e:
            logger.error(f"[ERROR] Failed to process file: {file}")
            logger.error(f"Reason: {e}")
            continue



if __name__=="__main__":
    years = [2021]
    for i in years:
        path = rf"C:\Users\Vivek\Desktop\custom\{i}"
        folder_path_wise_custom_data_import_in_db(path)