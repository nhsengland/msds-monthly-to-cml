from pyspark.sql.types import StructType, StructField, StringType

DIMENSIONS_SCHEMA = StructType([
    StructField("dimension_cohort_id",                  StringType(), nullable=False),
    StructField("metric_id",                            StringType(), nullable=False),
])


def create_dimensions_schema(dimensions: list[str]) -> StructType:
    dimension_fields = [StructField(col, StringType(), nullable=True) for col in dimensions]
    return StructType(DIMENSIONS_SCHEMA.fields + dimension_fields)
