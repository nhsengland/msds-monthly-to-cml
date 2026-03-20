import logging
import timeit
import sys
from datetime import datetime

from pyspark.sql import functions as F

from src.utils import file_paths
from src.utils import logging_config
from src.utils import spark as spark_utils
from src.data_ingestion import get_data
from src.data_ingestion import reading_data
from src.processing import processing
from src.processing import dimension_cohorts
from src.data_exports import write_csv
from src.schemas import dimensions as dim_schema, metric
from src.validation import validation

logger = logging.getLogger(__name__)

def main():
    # load config, here we load our project's parameters from the config.
    config = file_paths.get_config()
    # create spark session
    spark = spark_utils.create_spark_session(config['project_name'])
    # Loading data from CSV as spark data frame
    df_maternity = reading_data.load_csv_into_spark_data_frame(spark, config['path_to_maternity_data'])

    for processing_func_config in config["processing_funcs"]:
        processing_func = processing.PROCESSING_FUNC_REGISTRY[processing_func_config["name"]]
        df_maternity = processing_func(df_maternity, **processing_func_config["params"])

    df_maternity = dimension_cohorts.create_dimension_table(
        df_maternity,
        config["dimensions"],
        ["mbrrace_grouping"]
    )
    df_maternity = processing.concat_cols(df_maternity, "metric_dimension_id", ["metric_id", "dimension_cohort_id"], sep="_")

    dimensions_schema = dim_schema.create_dimensions_schema(config["dimensions"])
    df_dimensions = validation.select_from_schema(df_maternity, dimensions_schema)
    df_metric = validation.select_from_schema(df_maternity, metric.METRIC_SCHEMA)

    write_csv.save_df_as_named_csv(df_metric, "metric")
    write_csv.save_df_as_named_csv(df_dimensions, "dimensions")

    # stop the spark session
    spark.stop()
        

if __name__ == "__main__":
    print(f"Running create_publication script")
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    logger.info(f"Running time of create_publication script: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.\n")
