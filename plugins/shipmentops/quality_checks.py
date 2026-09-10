import os
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Callable
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

WAREHOUSE_CONN = os.environ.get(
    'WAREHOUSE_CONN', 
    'postgresql://warehouse:warehouse@postgres-warehouse:5432/shipmentops'
)


class DataQualityRule:
    
    def __init__(self, name: str, check_func: Callable, description: str):
        self.name = name
        self.check_func = check_func
        self.description = description
    
    def check(self, df: pd.DataFrame) -> Tuple[bool, int, str]:
        #Returns (passed, violation_count, details).
        
        try:
            violations = self.check_func(df)
            if isinstance(violations, pd.Series):
                count = violations.sum()
                if count > 0:
                    sample_rows = df[violations].head(5).to_dict('records')
                    return False, count, f"Sample violations: {sample_rows}"
                return True, 0, "Passed"
            return True, 0, "Passed"
        except Exception as e:
            return False, -1, f"Check failed with error: {str(e)}"


class DataQualityValidator:
    
    def __init__(self):
        self.rules: List[DataQualityRule] = []
    
    def add_rule(self, rule: DataQualityRule) -> 'DataQualityValidator':
        self.rules.append(rule)
        return self
    
    def validate(self, df: pd.DataFrame, dataset_name: str) -> List[Dict]:
        #Run all rules and collect results.
        results = []
        
        for rule in self.rules:
            passed, count, details = rule.check(df)
            result = {
                "dataset": dataset_name,
                "rule": rule.name,
                "description": rule.description,
                "passed": passed,
                "violation_count": count,
                "details": details,
                "checked_at": datetime.now().isoformat()
            }
            results.append(result)
            
            if not passed:
                logger.warning(f"DQ FAILED [{dataset_name}] {rule.name}: {count} violations")
            else:
                logger.info(f"DQ PASSED [{dataset_name}] {rule.name}")
        
        return results


# Rule helper functions
def not_null(column: str) -> Callable:
    return lambda df: df[column].isnull()


def unique_values(column: str) -> Callable:
    return lambda df: df.duplicated(subset=[column], keep=False)


def between(column: str, min_val, max_val) -> Callable:
    def check(df):
        # Handle non-numeric gracefully
        numeric_col = pd.to_numeric(df[column], errors='coerce')
        return (numeric_col < min_val) | (numeric_col > max_val)
    return check


def non_negative(column: str) -> Callable:
    def check(df):
        numeric_col = pd.to_numeric(df[column], errors='coerce')
        return numeric_col < 0
    return check


def in_set(column: str, valid_values: set) -> Callable:
    return lambda df: ~df[column].isin(valid_values) & df[column].notna()


def referential_integrity(fact_column: str, dim_table: str, dim_key: str) -> Callable:
    def check(df):
        engine = create_engine(WAREHOUSE_CONN)
        with engine.connect() as conn:
            dim_values = pd.read_sql(f"SELECT {dim_key} FROM {dim_table}", conn)
        
        valid_keys = set(dim_values[dim_key].dropna())
        fact_values = df[fact_column].dropna()
        
        return ~fact_values.isin(valid_keys)
    
    return check


def build_orders_validator() -> DataQualityValidator:
    validator = DataQualityValidator()
    
    validator.add_rule(DataQualityRule(
        "order_id_not_null",
        not_null("order_id"),
        "Order ID must not be null"
    ))
    
    validator.add_rule(DataQualityRule(
        "order_id_unique",
        unique_values("order_id"),
        "Order ID must be unique"
    ))
    
    validator.add_rule(DataQualityRule(
        "order_value_positive",
        non_negative("order_value_usd"),
        "Order value must be non-negative"
    ))
    
    validator.add_rule(DataQualityRule(
        "status_valid",
        in_set("status", {"pending", "shipped", "delivered", "cancelled"}),
        "Status must be a valid value"
    ))
    
    return validator


def build_delivery_events_validator() -> DataQualityValidator:
    validator = DataQualityValidator()
    
    validator.add_rule(DataQualityRule(
        "event_id_not_null",
        not_null("event_id"),
        "Event ID must not be null"
    ))
    
    validator.add_rule(DataQualityRule(
        "duration_non_negative",
        non_negative("delivery_duration_minutes"),
        "Delivery duration must be non-negative (physical constraint)"
    ))
    
    validator.add_rule(DataQualityRule(
        "event_type_valid",
        in_set("event_type", {"picked_up", "in_transit", "out_for_delivery", "delivered", "exception"}),
        "Event type must be valid"
    ))
    
    return validator


def build_warehouse_validator() -> DataQualityValidator:
    validator = DataQualityValidator()
    
    validator.add_rule(DataQualityRule(
        "quantity_non_negative",
        non_negative("quantity_on_hand"),
        "Quantity on hand must be non-negative"
    ))
    
    validator.add_rule(DataQualityRule(
        "reorder_level_positive",
        lambda df: pd.to_numeric(df["reorder_level"], errors='coerce') <= 0,
        "Reorder level must be positive"
    ))
    
    return validator


VALIDATORS = {
    "orders": build_orders_validator,
    "delivery_events": build_delivery_events_validator,
    "warehouse_inventory": build_warehouse_validator
}


def run_quality_checks(**context) -> Dict:
    execution_date = context['ds']
    ti = context['ti']
    
    transformed_files = ti.xcom_pull(task_ids='transform', key='transformed_files')
    
    if not transformed_files:
        raise ValueError("No transformed files found")
    
    all_results = []
    failures = []
    
    # Run dataset-level checks
    for source_name, filepath in transformed_files.items():
        if not os.path.exists(filepath):
            continue
        
        df = pd.read_parquet(filepath)
        validator_builder = VALIDATORS.get(source_name)
        
        if validator_builder:
            validator = validator_builder()
            results = validator.validate(df, source_name)
            all_results.extend(results)
            failures.extend([r for r in results if not r['passed']])
    
    # Run warehouse-level referential integrity checks
    engine = create_engine(WAREHOUSE_CONN)
    
    with engine.connect() as conn:
        # Check fact_deliveries referential integrity
        orphan_check = pd.read_sql("""
            SELECT COUNT(*) as orphan_count
            FROM fact_deliveries f
            LEFT JOIN dim_warehouse w ON f.warehouse_key = w.warehouse_key
            WHERE f.warehouse_key IS NOT NULL AND w.warehouse_key IS NULL
        """, conn)
        
        if orphan_check.iloc[0]['orphan_count'] > 0:
            failures.append({
                "dataset": "fact_deliveries",
                "rule": "warehouse_fk_integrity",
                "passed": False,
                "violation_count": int(orphan_check.iloc[0]['orphan_count']),
                "details": "Orphaned warehouse keys in fact table"
            })
    
    # Log summary
    logger.info(f"Quality checks complete: {len(all_results)} checks, {len(failures)} failures")
    
    # Raise error if critical failures (optional - could be soft fail)
    if failures:
        logger.warning(f"Quality check failures: {failures}")
        # For demo, we log but don't fail. In production, you might want to fail here.
    
    return {
        "total_checks": len(all_results),
        "failures": len(failures),
        "results": all_results
    }