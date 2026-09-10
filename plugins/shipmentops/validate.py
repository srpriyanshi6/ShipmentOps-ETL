import os
import logging
from datetime import datetime
from typing import Dict, List, Tuple
import pandas as pd

logger = logging.getLogger(__name__)

QUARANTINE_DIR = "/opt/airflow/data/quarantine"


SCHEMAS = {
    "orders": {
        "required_columns": ["order_id", "warehouse_id", "route_id", "order_date", "order_value", "status"],
        "nullable_columns": ["partner_id", "actual_delivery_date"],
        "unique_key": "order_id"
    },
    "warehouse_inventory": {
        "required_columns": ["inventory_id", "warehouse_id", "sku", "quantity_on_hand"],
        "nullable_columns": ["quantity_on_hand"],
        "unique_key": "inventory_id"
    },
    "delivery_events": {
        "required_columns": ["event_id", "order_id", "event_type", "event_timestamp"],
        "nullable_columns": ["delivery_duration_minutes"],
        "unique_key": "event_id"
    },
    "partner_payments": {
        "required_columns": ["payment_id", "partner_id", "payment_date", "payment_status"],
        "nullable_columns": ["amount"],
        "unique_key": "payment_id"
    }
}


def quarantine_records(df: pd.DataFrame, source: str, reason: str, execution_date: str):
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    quarantine_path = os.path.join(
        QUARANTINE_DIR, 
        f"{source}_quarantine_{execution_date}.csv"
    )
    
    df = df.copy()
    df['_quarantine_reason'] = reason
    df['_quarantined_at'] = datetime.now().isoformat()
    
    if os.path.exists(quarantine_path):
        df.to_csv(quarantine_path, mode='a', header=False, index=False)
    else:
        df.to_csv(quarantine_path, index=False)
    
    logger.warning(f"Quarantined {len(df)} records from {source}: {reason}")


def validate_schema(df: pd.DataFrame, source: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    schema = SCHEMAS.get(source, {})
    if not schema:
        logger.warning(f"No schema defined for {source}, skipping validation")
        return df, pd.DataFrame()
    
    required_cols = schema.get("required_columns", [])
    
    #missing columns
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns in {source}: {missing_cols}")
        #if critical columns are missing, quarantine all
        quarantine_records(df, source, f"Missing columns: {missing_cols}", "schema_error")
        return pd.DataFrame(), df
    
    #check nulls in required columns (excluding explicitly nullable)
    nullable_cols = schema.get("nullable_columns", [])
    non_nullable_required = [col for col in required_cols if col not in nullable_cols]
    
    if non_nullable_required:
        null_mask = df[non_nullable_required].isnull().any(axis=1)
        invalid_df = df[null_mask].copy()
        valid_df = df[~null_mask].copy()
        
        if len(invalid_df) > 0:
            #which columns have nulls
            null_cols = []
            for col in non_nullable_required:
                if invalid_df[col].isnull().any():
                    null_cols.append(col)
            quarantine_records(
                invalid_df, source, 
                f"Null values in required columns: {null_cols}", 
                "null_check"
            )
        
        return valid_df, invalid_df
    
    return df, pd.DataFrame()


def validate_uniqueness(df: pd.DataFrame, source: str) -> pd.DataFrame:
    schema = SCHEMAS.get(source, {})
    unique_key = schema.get("unique_key")
    
    if not unique_key or unique_key not in df.columns:
        return df
    
    duplicates = df[df.duplicated(subset=[unique_key], keep=False)]
    if len(duplicates) > 0:
        logger.info(f"Found {len(duplicates)} duplicate records in {source} based on {unique_key}")
        
        # Log duplicates to quarantine (but don't remove from main flow)
        dup_path = os.path.join(QUARANTINE_DIR, f"{source}_duplicates_{datetime.now().strftime('%Y%m%d')}.csv")
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        duplicates.to_csv(dup_path, index=False)
        
        #keeping first occurrence
        df = df.drop_duplicates(subset=[unique_key], keep='first')
    
    return df


def validate_all_sources(**context) -> Dict[str, str]:
    execution_date = context['ds']
    ti = context['ti']
    
    # Get extracted file paths from previous task
    extracted_files = ti.xcom_pull(task_ids='extract', key='extracted_files')
    
    if not extracted_files:
        raise ValueError("No extracted files found from extract task")
    
    validated_files = {}
    
    for source_name, filepath in extracted_files.items():
        if not os.path.exists(filepath):
            logger.warning(f"Processed file not found: {filepath}")
            continue
        
        df = pd.read_parquet(filepath)
        original_count = len(df)
        
        # Step 1: Schema validation
        df, _ = validate_schema(df, source_name)
        
        # Step 2: Uniqueness check
        df = validate_uniqueness(df, source_name)
        
        # Save validated data
        validated_path = filepath.replace("_processed", "_validated")
        #use same processed path pattern
        validated_path = os.path.join(
            "/opt/airflow/data/processed",
            f"{source_name}_validated_{execution_date}.parquet"
        )
        df.to_parquet(validated_path, index=False)
        
        validated_files[source_name] = validated_path
        logger.info(f"Validated {source_name}: {original_count} -> {len(df)} rows")
    
    return validated_files