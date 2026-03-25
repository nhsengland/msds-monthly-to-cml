import logging
import timeit
from datetime import datetime

from pyspark.sql import functions as F
from cml_conversion_helpers.processing import processing
from cml_conversion_helpers.processing import dimension_cohorts
from cml_schemas import spark_schemas

from msds_monthly_to_cml.utils import file_paths
from msds_monthly_to_cml.utils import logging_config
from msds_monthly_to_cml.utils import spark as spark_utils
from msds_monthly_to_cml.data_ingestion import reading_data
from msds_monthly_to_cml.data_exports import write_csv


logger = logging.getLogger(__name__)

def main():
    
    # load config - here we load our project's parameters from the config file.
    config = file_paths.get_config("config.yaml")
    

    # configure logging - we can save information to log files which can be useful for debugging with logger.info()
    logging_config.configure_logging(config['log_dir'])
    logger.info(f"Configured logging with log folder: {config['log_dir']}.")
    logger.info(f"Logging the config settings:\n\n\t{config}\n")
    logger.info(f"Starting run at:\t{datetime.now().time()}")


    # create spark session
    spark = spark_utils.create_spark_session(config['project_name'])
    logger.info(f"Created SparkSession with name: {config['project_name']}.")


    # Loading data from CSV as spark data frame
    df_maternity = reading_data.load_csv_into_spark_data_frame(spark, config['path_to_source_data'])
    logger.info(f"Loaded source data from: {config['path_to_source_data']}.")


    # loop through the processing functions defined in the config
    logger.info(f"running functions defined in config...")
    for processing_func_config in config["processing_funcs"]:
        logger.info(f"   running {processing_func_config['name']}")
        processing_func = processing.PROCESSING_FUNC_REGISTRY[processing_func_config["name"]]
        df_maternity = processing_func(df_maternity, **processing_func_config["params"])
    logger.info(f"done!")


    # create the columns needed for the dimensions table
    df_maternity = dimension_cohorts.create_dimension_table(
        df_maternity,
        config["dimensions"],
        config["dimension_creation_exclusions"]
    )
    df_maternity = processing.concat_cols(df_maternity, "metric_dimension_id", ["metric_id", "dimension_cohort_id"], sep="_")
    logger.info(f"created the columns needed for the dimensions table.")


    # now df_maternity has all the columns needed for the dimensions and metric tables. the spark_schemas module from the cml_schemas
    # package contains the schemas for each table. we can use the select_from_schema() function to select the columns
    # that belong to each schema, which leaves us with two new dataframes, one for each table.
    dimensions_schema = spark_schemas.create_dimensions_schema(config["dimensions"])
    df_dimensions = spark_schemas.select_from_schema(df_maternity, dimensions_schema)
    df_metric = spark_schemas.select_from_schema(df_maternity, spark_schemas.METRIC_SCHEMA)
    logger.info(f"created df_metric and df_dimensions")


    # Then we can save these to CSV
    logger.info(f"writing data to csv...")
    write_csv.save_df_as_named_csv(df_metric, "metric")
    write_csv.save_df_as_named_csv(df_dimensions, "dimensions")
    logger.info(f"   done!")


    # stop the spark session
    logger.info(f"stopping the SparkSession.")
    spark.stop()
        

if __name__ == "__main__":
    print(f"Running create_cml_tables script")
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    logger.info(f"Running time of create_cml_tables script: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.\n")
