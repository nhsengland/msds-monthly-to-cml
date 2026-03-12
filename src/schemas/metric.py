from pyspark.sql.types import StructType, StructField, StringType, TimestampType

METRIC_SCHEMA = StructType([
    StructField("datapoint_id",                     StringType(),    nullable=False),
    StructField("metric_id",                        StringType(),    nullable=False),
    StructField("metric_dimension_id",              StringType(),    nullable=False),
    StructField("dimension_cohort_id",              StringType(),    nullable=False),
    StructField("location_id",                      StringType(),    nullable=False),
    StructField("location_type",                    StringType(),    nullable=False),
    StructField("reporting_period_start_datetime",  TimestampType(), nullable=False),
    StructField("last_record_timestamp",            TimestampType(), nullable=False),
    StructField("last_ingest_timestamp",            TimestampType(), nullable=False),
    StructField("publication_date",                 TimestampType(), nullable=False),
    StructField("metric_value",                     StringType(),    nullable=True),
    StructField("additional_metric_values",         StringType(),    nullable=True),
])
