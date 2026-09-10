import os
import logging
from datetime import datetime
from typing import Dict, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Warehouse connection
WAREHOUSE_CONN = os.environ.get(
    'WAREHOUSE_CONN', 
    'postgresql://warehouse:warehouse@postgres-warehouse:5432/shipmentops'
)


def get_engine() -> Engine:
    return create_engine(WAREHOUSE_CONN) #SQLAlchemy engine


def get_watermark(engine: Engine, table_name: str) -> Optional[str]:
    query = text("""
        SELECT last_watermark 
        FROM etl_metadata 
        WHERE table_name = :table_name
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"table_name": table_name}).fetchone()
        if result:
            return result[0]
    return None


def update_watermark(engine: Engine, table_name: str, watermark: str):
    query = text("""
        INSERT INTO etl_metadata (table_name, last_watermark, updated_at)
        VALUES (:table_name, :watermark, :updated_at)
        ON CONFLICT (table_name) 
        DO UPDATE SET last_watermark = :watermark, updated_at = :updated_at
    """)
    
    with engine.begin() as conn:
        conn.execute(query, {
            "table_name": table_name,
            "watermark": watermark,
            "updated_at": datetime.now()
        })


def load_dim_warehouse(engine: Engine, df: pd.DataFrame):
    #Load warehouse dimension. Uses SCD Type 1 (upsert).

    # Get unique warehouses
    warehouses = df[['warehouse_id']].drop_duplicates()
    warehouses['warehouse_name'] = warehouses['warehouse_id'].apply(
        lambda x: f"Warehouse {x.split('_')[1]}"
    )
    warehouses['region'] = warehouses['warehouse_id'].apply(
        lambda x: ['North', 'South', 'East', 'West'][int(x.split('_')[1]) % 4]
    )
    
    # Upsert into dimension table
    with engine.begin() as conn:
        for _, row in warehouses.iterrows():
            conn.execute(text("""
                INSERT INTO dim_warehouse (warehouse_id, warehouse_name, region, created_at, updated_at)
                VALUES (:warehouse_id, :warehouse_name, :region, :now, :now)
                ON CONFLICT (warehouse_id) 
                DO UPDATE SET warehouse_name = :warehouse_name, region = :region, updated_at = :now
            """), {
                "warehouse_id": row['warehouse_id'],
                "warehouse_name": row['warehouse_name'],
                "region": row['region'],
                "now": datetime.now()
            })
    
    logger.info(f"Loaded {len(warehouses)} warehouses into dim_warehouse")


def load_dim_partner(engine: Engine, orders_df: pd.DataFrame, payments_df: pd.DataFrame):
    #from orders and payments.
    
    # Get unique partners
    partners_from_orders = orders_df[['partner_id']].dropna()
    partners_from_payments = payments_df[['partner_id']].dropna()
    partners = pd.concat([partners_from_orders, partners_from_payments]).drop_duplicates()
    
    with engine.begin() as conn:
        for _, row in partners.iterrows():
            partner_id = row['partner_id']
            conn.execute(text("""
                INSERT INTO dim_partner (partner_id, partner_name, partner_type, created_at, updated_at)
                VALUES (:partner_id, :partner_name, :partner_type, :now, :now)
                ON CONFLICT (partner_id) 
                DO UPDATE SET updated_at = :now
            """), {
                "partner_id": partner_id,
                "partner_name": f"Partner {partner_id.split('_')[1]}",
                "partner_type": "carrier",
                "now": datetime.now()
            })
    
    logger.info(f"Loaded {len(partners)} partners into dim_partner")


def load_dim_route(engine: Engine, df: pd.DataFrame):
    
    routes = df[['route_id']].drop_duplicates()
    routes['origin'] = routes['route_id'].apply(lambda x: f"City_{int(x.split('_')[1]) % 10}")
    routes['destination'] = routes['route_id'].apply(lambda x: f"City_{(int(x.split('_')[1]) + 5) % 10}")
    routes['distance_km'] = routes['route_id'].apply(lambda x: 100 + (int(x.split('_')[1]) * 10) % 500)
    
    with engine.begin() as conn:
        for _, row in routes.iterrows():
            conn.execute(text("""
                INSERT INTO dim_route (route_id, origin, destination, distance_km, created_at, updated_at)
                VALUES (:route_id, :origin, :destination, :distance_km, :now, :now)
                ON CONFLICT (route_id) 
                DO UPDATE SET updated_at = :now
            """), {
                "route_id": row['route_id'],
                "origin": row['origin'],
                "destination": row['destination'],
                "distance_km": row['distance_km'],
                "now": datetime.now()
            })
    
    logger.info(f"Loaded {len(routes)} routes into dim_route")


def load_dim_date(engine: Engine, start_date: str, end_date: str):
    #Populate date dimension with a date range.
    
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    with engine.begin() as conn:
        for dt in date_range:
            conn.execute(text("""
                INSERT INTO dim_date (date_key, full_date, year, quarter, month, day, day_of_week, is_weekend)
                VALUES (:date_key, :full_date, :year, :quarter, :month, :day, :day_of_week, :is_weekend)
                ON CONFLICT (date_key) DO NOTHING
            """), {
                "date_key": int(dt.strftime('%Y%m%d')),
                "full_date": dt.date(),
                "year": dt.year,
                "quarter": dt.quarter,
                "month": dt.month,
                "day": dt.day,
                "day_of_week": dt.dayofweek,
                "is_weekend": dt.dayofweek >= 5
            })
    
    logger.info(f"Populated dim_date from {start_date} to {end_date}")


def load_fact_deliveries(engine: Engine, orders_df: pd.DataFrame, events_df: pd.DataFrame):
    
    # Get watermark for incremental loading
    watermark = get_watermark(engine, 'fact_deliveries')
    
    if watermark:
        # Only load records newer than watermark
        orders_df = orders_df[orders_df['order_date'] > watermark]
        logger.info(f"Incremental load: processing {len(orders_df)} new orders since {watermark}")
    
    if len(orders_df) == 0:
        logger.info("No new records to load")
        return
    
    # Join with dimension keys
    with engine.connect() as conn:
        # Get dimension lookups
        warehouses = pd.read_sql("SELECT warehouse_key, warehouse_id FROM dim_warehouse", conn)
        partners = pd.read_sql("SELECT partner_key, partner_id FROM dim_partner", conn)
        routes = pd.read_sql("SELECT route_key, route_id FROM dim_route", conn)
    
    # Merge to get surrogate keys
    orders_df = orders_df.merge(warehouses, on='warehouse_id', how='left')
    orders_df = orders_df.merge(partners, on='partner_id', how='left')
    orders_df = orders_df.merge(routes, on='route_id', how='left')
    
    # Add date keys
    orders_df['order_date_key'] = pd.to_datetime(orders_df['order_date'], errors='coerce').dt.strftime('%Y%m%d').astype(float).astype('Int64')
    orders_df['promised_date_key'] = pd.to_datetime(orders_df['promised_delivery_date'], errors='coerce').dt.strftime('%Y%m%d').astype(float).astype('Int64')
    orders_df['actual_date_key'] = pd.to_datetime(orders_df['actual_delivery_date'], errors='coerce').dt.strftime('%Y%m%d').astype(float).astype('Int64')
    
    # Calculate delivery metrics
    orders_df['delivery_duration_hours'] = orders_df['days_late'] * 24
    
    # Prepare fact records
    fact_records = orders_df[[
        'order_id', 'warehouse_key', 'partner_key', 'route_key',
        'order_date_key', 'promised_date_key', 'actual_date_key',
        'order_value_usd', 'is_on_time', 'days_late', 'delivery_duration_hours'
    ]].copy()
    
    # Insert into fact table
    with engine.begin() as conn:
        for _, row in fact_records.iterrows():
            conn.execute(text("""
                INSERT INTO fact_deliveries (
                    order_id, warehouse_key, partner_key, route_key,
                    order_date_key, promised_date_key, actual_date_key,
                    order_value_usd, is_on_time, days_late, delivery_duration_hours,
                    created_at
                ) VALUES (
                    :order_id, :warehouse_key, :partner_key, :route_key,
                    :order_date_key, :promised_date_key, :actual_date_key,
                    :order_value_usd, :is_on_time, :days_late, :delivery_duration_hours,
                    :created_at
                )
                ON CONFLICT (order_id) DO UPDATE SET
                    is_on_time = :is_on_time,
                    days_late = :days_late,
                    delivery_duration_hours = :delivery_duration_hours
            """), {
                "order_id": row['order_id'],
                "warehouse_key": int(row['warehouse_key']) if pd.notna(row['warehouse_key']) else None,
                "partner_key": int(row['partner_key']) if pd.notna(row['partner_key']) else None,
                "route_key": int(row['route_key']) if pd.notna(row['route_key']) else None,
                "order_date_key": int(row['order_date_key']) if pd.notna(row['order_date_key']) else None,
                "promised_date_key": int(row['promised_date_key']) if pd.notna(row['promised_date_key']) else None,
                "actual_date_key": int(row['actual_date_key']) if pd.notna(row['actual_date_key']) else None,
                "order_value_usd": float(row['order_value_usd']) if pd.notna(row['order_value_usd']) else None,
                "is_on_time": bool(row['is_on_time']) if pd.notna(row['is_on_time']) else None,
                "days_late": int(row['days_late']) if pd.notna(row['days_late']) else None,
                "delivery_duration_hours": float(row['delivery_duration_hours']) if pd.notna(row['delivery_duration_hours']) else None,
                "created_at": datetime.now()
            })
    
    # Update watermark
    max_date = orders_df['order_date'].max()
    if max_date:
        update_watermark(engine, 'fact_deliveries', max_date)
    
    logger.info(f"Loaded {len(fact_records)} records into fact_deliveries")


def load_all(**context):
    execution_date = context['ds']
    ti = context['ti']
    
    transformed_files = ti.xcom_pull(task_ids='transform', key='transformed_files')
    
    if not transformed_files:
        raise ValueError("No transformed files found")
    
    engine = get_engine()
    
    # Load dataframes
    dfs = {}
    for source, path in transformed_files.items():
        if os.path.exists(path):
            dfs[source] = pd.read_parquet(path)
    
    # Load dimensions first (order matters for foreign keys)
    if 'warehouse_inventory' in dfs or 'orders' in dfs:
        source_df = dfs.get('orders', dfs.get('warehouse_inventory'))
        if source_df is not None and 'warehouse_id' in source_df.columns:
            load_dim_warehouse(engine, source_df)
    
    if 'orders' in dfs and 'partner_payments' in dfs:
        load_dim_partner(engine, dfs['orders'], dfs['partner_payments'])
    
    if 'orders' in dfs and 'route_id' in dfs['orders'].columns:
        load_dim_route(engine, dfs['orders'])
    
    # Populate date dimension (last 2 years to next year)
    start_date = (datetime.now() - pd.Timedelta(days=730)).strftime('%Y-%m-%d')
    end_date = (datetime.now() + pd.Timedelta(days=365)).strftime('%Y-%m-%d')
    load_dim_date(engine, start_date, end_date)
    
    # Load fact table
    if 'orders' in dfs:
        events_df = dfs.get('delivery_events', pd.DataFrame())
        load_fact_deliveries(engine, dfs['orders'], events_df)
    
    logger.info("Load complete")