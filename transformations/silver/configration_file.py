dq_path="/Volumes/transportation/bronze/dq_rules/trips_dq_rules.json"



import json
def load_config():
    path = dq_path
    with open(path, "r") as f:
        config = json.load(f)
    schema_config = config["schema"]
    rules = config["rules"]
    table_name = config["table_name"]
    return schema_config, rules, table_name

from pyspark.sql.functions import col, expr

def apply_schema(df, schema_config):
    for column, dtype in schema_config.items():
        df = df.withColumn(column, col(column).cast(dtype))
    return df

def apply_dq(df, rules):
    dq_rules = " AND ".join(rules.values())
    return df.withColumn("is_valid", expr(dq_rules))
     