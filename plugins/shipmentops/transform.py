import os
import logging
from datetime import datetime
from typing import Dict, Optional
import pandas as pd
import numpy as np
from dateutil import parser as date_parser

logger = logging.getLogger(__name__)

PROCESSED_DIR = "/opt/airflow/data/processed"

# Currency exchange rates (static for demo)
EXCHANGE_RATES = {
    'USD': 1.0,
    'EUR': 1.10,
    'GBP': 1.27
}


def standardize_date(date_str: str) -> Optional[str]:
    if pd.isna(date_str) or date_str is None or str(date_str).strip() == '':
        return None
    
    try:
        parsed = date_parser.parse(str(date_str), dayfirst=False)
        return parsed.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None


def standardize_datetime(dt_str: str) -> Optional[str]:
    if pd.isna(dt_str) or dt_str is None or str(dt_str).strip() == '':
        return None
    
    try:
        parsed = date_parser.parse(str(dt_str))
        return parsed.isoformat()
    except (ValueError, TypeError):
        return None


def standardize_currency(row: pd.Series, amount_col: str, currency_col: str) -> float:
    amount = row.get(amount_col)
    currency = row.get(currency_col, 'USD')
    
    if pd.isna(amount):
        return None
    
    try:
        amount = float(amount)
        rate = EXCHANGE_RATES.get(str(currency).upper(), 1.0)
        return round(amount * rate, 2)
    except (ValueError, TypeError):
        return None


def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    #standardize dates
    df['order_date'] = df['order_date'].apply(standardize_date)
    df['promised_delivery_date'] = df['promised_delivery_date'].apply(standardize_date)
    df['actual_delivery_date'] = df['actual_delivery_date'].apply(standardize_date)
    
    #standardize status (lowercase, strip whitespace)
    df['status'] = df['status'].str.lower().str.strip()
    
    # Convert order_value to USD
    df['order_value_usd'] = df.apply(
        lambda row: standardize_currency(row, 'order_value', 'currency'), 
        axis=1
    )
    
    #calculate delivery metrics
    df['promised_delivery_date'] = pd.to_datetime(df['promised_delivery_date'], errors='coerce')
    df['actual_delivery_date'] = pd.to_datetime(df['actual_delivery_date'], errors='coerce')
    
    df['days_late'] = (df['actual_delivery_date'] - df['promised_delivery_date']).dt.days
    df['is_on_time'] = df['days_late'].apply(lambda x: True if pd.notna(x) and x <= 0 else (False if pd.notna(x) else None))
    
    # Convert dates back to string for consistency
    df['promised_delivery_date'] = df['promised_delivery_date'].dt.strftime('%Y-%m-%d')
    df['actual_delivery_date'] = df['actual_delivery_date'].dt.strftime('%Y-%m-%d')
    
    return df


def transform_delivery_events(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    #standardize timestamps
    df['event_timestamp'] = df['event_timestamp'].apply(standardize_datetime)
    
    #standardize event type
    df['event_type'] = df['event_type'].str.lower().str.strip()
    
    #flag negative durations (invalid)
    df['delivery_duration_minutes'] = pd.to_numeric(df['delivery_duration_minutes'], errors='coerce')
    df['is_valid_duration'] = df['delivery_duration_minutes'].apply(
        lambda x: True if pd.isna(x) or x >= 0 else False
    )
    
    #set negative durations to null
    df.loc[df['delivery_duration_minutes'] < 0, 'delivery_duration_minutes'] = None
    
    return df


def transform_partner_payments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    #standardize dates
    df['payment_date'] = df['payment_date'].apply(standardize_date)
    
    #standardize status
    df['payment_status'] = df['payment_status'].str.lower().str.strip()
    
    #convert amount to USD
    df['amount_usd'] = df.apply(
        lambda row: standardize_currency(row, 'amount', 'currency'), 
        axis=1
    )
    
    return df


def transform_warehouse_inventory(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Convert numeric fields
    df['quantity_on_hand'] = pd.to_numeric(df['quantity_on_hand'], errors='coerce')
    df['reorder_level'] = pd.to_numeric(df['reorder_level'], errors='coerce')
    
    #standardize datetime
    df['last_updated'] = df['last_updated'].apply(standardize_datetime)
    
    #calculate if below reorder level
    df['needs_reorder'] = df.apply(
        lambda row: True if pd.notna(row['quantity_on_hand']) and 
                           pd.notna(row['reorder_level']) and 
                           row['quantity_on_hand'] < row['reorder_level'] 
                  else False,
        axis=1
    )
    
    return df


TRANSFORM_FUNCTIONS = {
    "orders": transform_orders,
    "delivery_events": transform_delivery_events,
    "partner_payments": transform_partner_payments,
    "warehouse_inventory": transform_warehouse_inventory
}


def transform_all_sources(**context) -> Dict[str, str]:
    execution_date = context['ds']
    ti = context['ti']
    
    validated_files = ti.xcom_pull(task_ids='validate', key='validated_files')
    
    if not validated_files:
        raise ValueError("No validated files found from validate task")
    
    transformed_files = {}
    
    for source_name, filepath in validated_files.items():
        if not os.path.exists(filepath):
            logger.warning(f"Validated file not found: {filepath}")
            continue
        
        df = pd.read_parquet(filepath)
        
        #apply transformation
        transform_func = TRANSFORM_FUNCTIONS.get(source_name)
        if transform_func:
            df = transform_func(df)
        
        #save data
        transformed_path = os.path.join(
            PROCESSED_DIR,
            f"{source_name}_transformed_{execution_date}.parquet"
        )
        df.to_parquet(transformed_path, index=False)
        
        transformed_files[source_name] = transformed_path
        logger.info(f"Transformed {source_name}: {len(df)} rows")
    
    return transformed_files