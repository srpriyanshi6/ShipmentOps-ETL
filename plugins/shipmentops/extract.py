"""
Extract module for ShipmentOps ETL.
Reads raw CSV files from the landing zone.
"""
import os
import logging
from datetime import datetime
from typing import Dict, List
import pandas as pd

logger = logging.getLogger(__name__)


LANDING_DIR = "/opt/airflow/data/landing"
QUARANTINE_DIR = "/opt/airflow/data/quarantine"
PROCESSED_DIR = "/opt/airflow/data/processed"

SOURCE_FILES = {
    "orders": "orders.csv",
    "warehouse_inventory": "warehouse_inventory.csv",
    "delivery_events": "delivery_events.csv",
    "partner_payments": "partner_payments.csv"
}


def get_landing_path(filename: str) -> str:
    return os.path.join(LANDING_DIR, filename)


def read_csv_safely(filepath: str) -> pd.DataFrame:
    try:
        # First pass: try reading all as strings to preserve raw data
        df = pd.read_csv(
            filepath,
            dtype=str,  # Read everything as string to handle inconsistent formats
            on_bad_lines='warn',  # Warn on malformed lines instead of failing
            encoding='utf-8'
        )
        logger.info(f"Successfully read {len(df)} rows from {filepath}")
        return df
    except Exception as e:
        logger.error(f"Failed to read {filepath}: {str(e)}")
        raise


def extract_all_sources(**context) -> Dict[str, str]:
    execution_date = context['ds']  # YYYY-MM-DD format
    extracted_files = {}
    
    for source_name, filename in SOURCE_FILES.items():
        filepath = get_landing_path(filename)
        
        if not os.path.exists(filepath):
            logger.warning(f"Source file not found: {filepath}")
            continue
            
        df = read_csv_safely(filepath)
        
        df['_extracted_at'] = datetime.now().isoformat()
        df['_source_file'] = filename
        df['_execution_date'] = execution_date
        
        processed_path = os.path.join(PROCESSED_DIR, f"{source_name}_{execution_date}.parquet")
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        df.to_parquet(processed_path, index=False)
        
        extracted_files[source_name] = processed_path
        logger.info(f"Extracted {source_name}: {len(df)} rows -> {processed_path}")
    
    return extracted_files


def generate_synthetic_data(num_days: int = 30):

    #Generate synthetic messy logistics data for testing.
    #realistic data with intentional quality issues.

    import random
    from faker import Faker
    
    fake = Faker()
    os.makedirs(LANDING_DIR, exist_ok=True)
    
    # Warehouses
    warehouses = [f"WH_{i:03d}" for i in range(1, 11)]
    
    # Partners (carriers)
    partners = [f"PARTNER_{i:03d}" for i in range(1, 21)]
    
    # Routes
    routes = [f"ROUTE_{i:03d}" for i in range(1, 51)]
    
    # Generate orders
    orders = []
    for i in range(1000):
        order = {
            "order_id": f"ORD_{i:06d}",
            "warehouse_id": random.choice(warehouses),
            "partner_id": random.choice(partners) if random.random() > 0.05 else None,  # 5% null
            "route_id": random.choice(routes),
            "order_date": fake.date_between(start_date='-30d', end_date='today').strftime(
                random.choice(['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y'])  # Inconsistent formats
            ),
            "promised_delivery_date": fake.date_between(start_date='today', end_date='+7d').strftime('%Y-%m-%d'),
            "actual_delivery_date": fake.date_between(start_date='-30d', end_date='today').strftime('%Y-%m-%d') if random.random() > 0.1 else None,
            "order_value": round(random.uniform(10, 5000), 2),
            "currency": random.choice(['USD', 'EUR', 'GBP', 'USD', 'USD']),  # Mostly USD
            "status": random.choice(['pending', 'shipped', 'delivered', 'cancelled', 'DELIVERED', 'Pending'])  # Inconsistent casing
        }
        orders.append(order)
    
    #add some duplicates
    orders.extend(random.sample(orders, 20))
    
    #generate warehouse inventory
    inventory = []
    for wh in warehouses:
        for _ in range(50):
            inv = {
                "inventory_id": f"INV_{fake.uuid4()[:8]}",
                "warehouse_id": wh,
                "sku": f"SKU_{random.randint(1000, 9999)}",
                "quantity_on_hand": random.randint(0, 1000) if random.random() > 0.03 else None,
                "reorder_level": random.randint(10, 100),
                "last_updated": fake.date_time_between(start_date='-7d', end_date='now').isoformat()
            }
            inventory.append(inv)
    
    #generate delivery events
    delivery_events = []
    for order in orders[:500]:  # Not all orders have events
        num_events = random.randint(1, 5)
        for j in range(num_events):
            event = {
                "event_id": f"EVT_{fake.uuid4()[:8]}",
                "order_id": order["order_id"],
                "event_type": random.choice(['picked_up', 'in_transit', 'out_for_delivery', 'delivered', 'exception']),
                "event_timestamp": fake.date_time_between(start_date='-30d', end_date='now').isoformat(),
                "location": fake.city(),
                "delivery_duration_minutes": random.randint(-10, 480) if random.random() > 0.05 else None  # Some negative (invalid)
            }
            delivery_events.append(event)
    
    #generate partner payments
    payments = []
    for i in range(300):
        payment = {
            "payment_id": f"PAY_{i:06d}",
            "partner_id": random.choice(partners),
            "amount": round(random.uniform(100, 10000), 2) if random.random() > 0.02 else None,
            "currency": random.choice(['USD', 'EUR', 'GBP']),
            "payment_date": fake.date_between(start_date='-60d', end_date='today').strftime('%Y-%m-%d'),
            "payment_status": random.choice(['completed', 'pending', 'failed', 'COMPLETED']),
            "reference_order_ids": ",".join([f"ORD_{random.randint(0, 999):06d}" for _ in range(random.randint(1, 5))])
        }
        payments.append(payment)
    
    #save to CSV files
    pd.DataFrame(orders).to_csv(os.path.join(LANDING_DIR, "orders.csv"), index=False)
    pd.DataFrame(inventory).to_csv(os.path.join(LANDING_DIR, "warehouse_inventory.csv"), index=False)
    pd.DataFrame(delivery_events).to_csv(os.path.join(LANDING_DIR, "delivery_events.csv"), index=False)
    pd.DataFrame(payments).to_csv(os.path.join(LANDING_DIR, "partner_payments.csv"), index=False)
    
    logger.info(f"Generated synthetic data in {LANDING_DIR}")