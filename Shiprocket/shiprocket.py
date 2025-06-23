import time
from typing import Optional ,List, Dict
import requests
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
import json
from dateutil.parser import parse
import numpy as np
from xlwings import view
import logging
import re
import warnings


current_date = datetime.today().date()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



load_dotenv()

SHIPROCKET_EMAIL = os.getenv("SHIPROCKET_EMAIL")
SHIPROCKET_PASSWORD = os.getenv("SHIPROCKET_PASSWORD")
LOGIN_URL = "https://apiv2.shiprocket.in/v1/external/auth/login"
SHIPROCKET_ORDERS_URL = 'https://apiv2.shiprocket.in/v1/external/orders'





_token_cache = {
    'token': None,
    'timestamp': None
    }


TOKEN_EXPIRY = timedelta(minutes=55)

ProductRenameFinal = {
    'name':"item_name",
    'channel_sku':"item_sku",
    'quantity':"item_quantity",
    'available':"item_available",
    'price':"item_price",
    'product_cost':"item_cost",
    'hsn':"item_hsn_code",
    'discount':"item_discount",
    'discount_including_tax':"item_discount_including_tax",
    'selling_price':"item_selling_price",
    'mrp':"item_mrp",
    'tax_percentage':"tax_percent",
    }


ShipmentRenameFinal = {
    'product_quantity':"shipment_quantity",
    'total':"shipment_total",
}


rename__final_dict = {
    "awb_data.charges.cod_charges": "cod_charges",
    "awb_data.charges.applied_weight_amount": "applied_weight_amount",
    "awb_data.charges.freight_charges": "freight_charges",
    "awb_data.charges.applied_weight": "applied_weight",
    "awb_data.charges.charged_weight": "charged_weight",
    "awb_data.charges.charged_weight_amount": "charged_weight_amount",
    "awb_data.charges.charged_weight_amount_rto": "charged_weight_amount_rto",
    "awb_data.charges.applied_weight_amount_rto": "applied_weight_amount_rto",
    "awb_data.charges.billing_amount": "billing_amount",
    
    # Total & Discounts
    "total": "order_total",
    "discount": "other_deduction",
    
    # Item-level fields
    "item_price": "item_net_price_excl_deduction",
    "item_cost": "item_sp_excl_tax",
    "item_discount": "item_disc_excl_tax",
    "item_selling_price": "item_sp_incl_tax",
    "item_discount_including_tax": "item_disc_incl_tax",
    
    # Timestamps
    "created_at": "shiprocket_created_at"
}


def get_shiprocket_token(force_refresh: bool = False) -> Optional[str]:

    if not force_refresh and _token_cache['token']:
        last_updated = datetime.fromtimestamp(_token_cache['timestamp'])
        if datetime.now() - last_updated < TOKEN_EXPIRY:
            return _token_cache['token']
    
    if not SHIPROCKET_EMAIL or not SHIPROCKET_PASSWORD:
        print("Missing Shiprocket credentials in environment variables")
        return None
    
    try:
        response = requests.post(
            LOGIN_URL,
            headers={"Content-Type": "application/json"},
            json={"email": SHIPROCKET_EMAIL, "password": SHIPROCKET_PASSWORD},
            timeout=10
        )
        
        if response.status_code == 200:
            token_data = response.json()
            token = token_data.get('token')
            
            if token:
                _token_cache['token'] = token
                _token_cache['timestamp'] = time.time()
                print("Shiprocket token generated successfully.")
                return token
            else:
                print("Token missing in response:", token_data)
                return None
    
        print(f"Failed to fetch token. Status: {response.status_code}")
        print("Response:", response.text)
        return None

    except requests.RequestException as e:
        print(f"Network error while fetching token: {str(e)}")
        return None

def get_orders(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    channel_id: Optional[int] = None,
    page: Optional[int] = None,
    per_page: Optional[int] = None,
    sort: Optional[str] = None,
    sort_by: Optional[str] = None,
    filter_by: Optional[str] = None,
    filter_value: Optional[str] = None,
    search: Optional[str] = None,
    pickup_location: Optional[str] = None,
    fbs: Optional[int] = None,
    debug: bool = False
) -> pd.DataFrame:
    
    
    warnings.filterwarnings("ignore", category=UserWarning)

    token = get_shiprocket_token()
    if not token:
        logging.error("Failed to get Shiprocket token.")
        return pd.DataFrame()

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    params = {}
    if from_date: params["from"] = from_date
    if to_date: params["to"] = to_date
    if channel_id is not None: params["channel_id"] = channel_id
    if page is not None: params["page"] = page
    if per_page is not None: params["per_page"] = per_page
    if sort: params["sort"] = sort
    if sort_by: params["sort_by"] = sort_by
    if filter_by: params["filter_by"] = filter_by
    if filter_value: params["filter"] = filter_value
    if search: params["search"] = search
    if pickup_location: params["pickup_location"] = pickup_location
    if fbs is not None: params["fbs"] = fbs

    for attempt in range(3):
        response = requests.get(SHIPROCKET_ORDERS_URL, headers=headers, params=params)
        if response.status_code == 200:
            break
        logging.warning(f"API attempt {attempt+1} failed. Retrying...")
    else:
        logging.error(f"API request failed after 3 attempts: {response.status_code}: {response.text}")
        return pd.DataFrame()

    try:
        data = response.json().get("data", [])

        if debug:
            with open('shiprocket_orders.json', 'w') as f:
                json.dump(data, f, indent=4)

        if not data:
            logging.info("No orders returned.")
            return pd.DataFrame()

        for order in data:
            others = order.get("others")
            if isinstance(others, dict):
                order["others"] = [others]
            elif not isinstance(others, list):
                order["others"] = None

        df_orders = pd.json_normalize(data)

        if any("products" in order and order["products"] for order in data):
            df_products = pd.json_normalize(data, record_path='products', meta=['id'], record_prefix='product_')\
                .drop(columns=["product_id"], errors='ignore')\
                .rename(columns={"product_quantity": "quantity"})
            df_products.columns = df_products.columns.str.removeprefix("product_")
            df_products.rename(columns=ProductRenameFinal,inplace=True)
            product_col = [
                "item_name","item_sku","item_quantity","item_available",
                "item_price","item_cost","item_hsn_code",
                "item_discount","item_discount_including_tax",
                "item_selling_price","item_mrp","tax_percent","description","id"
                ]
            df_products = df_products[product_col]
            numeric_columns = ["item_price", "item_cost", "item_hsn_code"]
            df_products[numeric_columns] = df_products[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0)


        else:
            df_products = pd.DataFrame(columns=["id", "product_name", "quantity"])

        if any("shipments" in order and order["shipments"] for order in data):
            df_shipments = pd.json_normalize(data, record_path='shipments', meta=['id'], record_prefix='shipment_')\
                .drop(columns=['shipment_id'], errors='ignore')
            df_shipments.columns = df_shipments.columns.str.removeprefix("shipment_")
            df_shipments = df_shipments.rename(columns=ShipmentRenameFinal)
            shipment_col = [
                "courier", "weight", "dimensions", "pickedup_timestamp","awb",
                "rto_delivered_date", "rto_initiated_date", "delivery_executive_name",
                "id"]
            df_shipments = df_shipments[shipment_col]
            df_shipments['weight'] = df_shipments['weight'].str.extract(r'(\d+\.?\d*)').astype(float).fillna(0)
        else:
            df_shipments = pd.DataFrame(columns=["id"])

        if any("others" in order and isinstance(order["others"], list) and order["others"] for order in data):
            df_others = pd.json_normalize(data, record_path=['others'], meta=['id'], record_prefix='others_', errors='ignore')\
                .drop(columns=["others_order_items"], errors='ignore')
            df_others.columns = df_others.columns.str.removeprefix("shipment_")
            
        else:
            df_others = pd.DataFrame(columns=["id"])

        df_final = df_orders.drop(columns=['products','shipments','others','activities','errors'], errors='ignore')
        df_final = df_final.merge(df_products, on='id', how='left')
        df_final = df_final.merge(df_shipments, on='id', how='left')
        df_final = df_final.rename(columns={'id':"shiprocket_id"})
        df_final.rename(columns=rename__final_dict, inplace=True,errors='ignore')
        final_cols = [
            'shiprocket_id', 'channel_order_id', 'shiprocket_created_at', 'invoice_no',
            'customer_name', 'customer_email', 'customer_phone', 'customer_address',
            'customer_address_2', 'customer_city', 'customer_state', 'customer_pincode',
            'status', 'payment_method', 'item_name', 'tax_percent',
            'item_quantity', 'item_net_price_excl_deduction', 'item_sp_excl_tax', 'item_disc_excl_tax',
            'item_sp_incl_tax', 'item_disc_incl_tax', 'order_total', 'other_deduction',
            'picked_up_date', 'etd_date', 'out_for_delivery_date', 'delivered_date',
            'rto_initiated_date', 'rto_delivered_date', 'cod_charges', 'applied_weight_amount',
            'freight_charges', 'charged_weight_amount', 'charged_weight_amount_rto', 'applied_weight_amount_rto',
            'billing_amount', 'other_charges', 'giftwrap_charges', 'courier',
            'weight', 'dimensions', 'applied_weight', 'charged_weight',
            'pickedup_timestamp', 'awb', 'delivery_executive_name', 'rto_risk',
            'pickup_location'
        ]

        for i in final_cols:
            if i not in df_final.columns:
                df_final[i] = ''
        df_final = df_final[final_cols]
        
        pattern = r'\bsurface\w*\b|\b\d+\s?(kg|kgs|gm|gms)\b'

        df_final["courier"] = df_final["courier"].str.replace(pattern, '', flags=re.IGNORECASE, regex=True).str.strip()

        date_columns = ["shiprocket_created_at","picked_up_date","etd_date","out_for_delivery_date","rto_initiated_date","rto_delivered_date","pickedup_timestamp","delivered_date"]
        custom_formats = {
            "shiprocket_created_at": "%d %b %Y, %I:%M %p",
            "pickedup_timestamp": "%d %b %Y, %I:%M %p",
            "etd_date": "%d-%m-%Y %H:%M:%S",
            "out_for_delivery_date": "%d-%m-%Y %H:%M:%S"
        }
        for col in date_columns:
            if col in custom_formats:
                df_final[col] = pd.to_datetime(df_final[col], format=custom_formats[col], errors="coerce")
            else:
                df_final[col] = pd.to_datetime(df_final[col], errors="coerce", dayfirst=True)

        for col in date_columns:
            df_final[col] = df_final[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        date_zero_replace = ['pickedup_timestamp', 'rto_initiated_date', 'rto_delivered_date']
        df_final[date_zero_replace] = df_final[date_zero_replace].replace({'0000-00-00 00:00:00': pd.NaT, '': pd.NaT})

        num_col = [
            "cod_charges", "applied_weight_amount", "freight_charges", "charged_weight_amount",
            "charged_weight_amount_rto", "applied_weight_amount_rto", "billing_amount", "other_charges",
            "giftwrap_charges", "applied_weight", "charged_weight",'order_total']
        df_final[num_col] = df_final[num_col].apply(pd.to_numeric, errors='coerce').fillna(0)

        groups_key = ['shiprocket_id','channel_order_id']
        one_time_value = [
            'order_total', 'other_deduction', 'cod_charges', 'applied_weight_amount',
            'freight_charges', 'charged_weight_amount', 'charged_weight_amount_rto',
            'applied_weight_amount_rto', 'billing_amount', 'other_charges',
            'giftwrap_charges', 'applied_weight', 'weight', 'charged_weight'
        ]

        valid_columns = [col for col in one_time_value if col in df_final.columns]

        first_mask = df_final.groupby(groups_key).cumcount() == 0
        df_final[valid_columns] = df_final[valid_columns].where(first_mask, 0)
        
        return df_final

    except ValueError as e:
        logging.error(f"Error parsing JSON response: {e}")
        return pd.DataFrame()

def get_all_orders(start_date: str, end_date: str) -> pd.DataFrame:
    all_orders = []
    page = 1
    while True:
        df = get_orders(
            from_date=start_date,
            to_date=end_date,
            per_page=100,
            page=page
        )
        if df.empty:
            break
        all_orders.append(df)
        print(f"Page {page} fetched with {len(df)} records.")
        page += 1
    return pd.concat(all_orders, ignore_index=True) if all_orders else pd.DataFrame()



