from models.base import KBEBase
from sqlalchemy import Column, Integer, String, Float, Date, DateTime
from sqlalchemy.sql import func

class KBEImportExport(KBEBase):
    __tablename__ = 'kbe_import_export'

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    hs_code = Column(String(20))
    product_description = Column(String(1000))
    quantity = Column(Float)
    unit = Column(String(10))
    fob_value_inr = Column(Float)
    unit_price_inr = Column(Float)
    fob_value_usd = Column(Float)
    fob_value_foreign_currency = Column(Float)
    unit_price_foreign_currency = Column(Float)
    currency_name = Column(String(10))
    fob_value_in_lacs_inr = Column(Float)
    iec = Column(String(20))
    indian_exporter_name = Column(String(255))
    exporter_address = Column(String(500))
    exporter_city = Column(String(100))
    pin_code = Column(String(10))
    cha_code = Column(String(20))
    cha_name = Column(String(255))
    foreign_importer_name = Column(String(255))
    importer_address = Column(String(500))
    importer_country = Column(String(100))
    foreign_port = Column(String(100))
    foreign_country = Column(String(100))
    indian_port = Column(String(100))
    item_no = Column(Integer)
    drawback = Column(Float)
    chapter = Column(String(10))
    hs_4_digit = Column(String(10))
    month = Column(String(15))
    year = Column(Integer)
    created_at = Column(DateTime, server_default=func.now())




