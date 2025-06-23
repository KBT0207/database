from models.base import KBEBase, KBBIOBase
from models.kbe.kbe_models import KBEImportExport
from models.shiprocket.shiprocket_models import ShiprocketOrder
import pandas as pd
from sql_connector import  kbbio_engine, kbe_engine, kbbio_connector, kbe_connector
from dbcrud import DatabaseCrud
from Shiprocket.shiprocket import get_all_orders
from datetime import datetime, date, timedelta
from logging_config import logger
from kbexports.kbe_processor import custom_data_processor

def shiprocket_daily():
    KBBIOBase.metadata.create_all(bind=kbbio_engine)
    db_kbbio = DatabaseCrud(kbbio_connector)

    from_date = datetime.today() - timedelta(days=500)
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
            delete_count = db_kbbio.delete_shiprocket_id_wise(shiprocket_ids)
            logger.info(f"Deleted {delete_count} existing records from DB.")
        else:
            logger.info("No Shiprocket IDs found in data. Skipping deletion step.")
        
        db_kbbio.import_data(table_name='shiprocket_orders',df=df,commit=True)

    except Exception as e:
        logger.error("Shiprocket sync failed", exc_info=True)

def kbe_custom_import_export(file:str):
    df = custom_data_processor(file_path=file)
    return df



if __name__ == "__main__":
    shiprocket_daily()
