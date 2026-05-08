import logging
import timeit
from datetime import datetime
from pathlib import Path

import pandas as pd

from cml_conversion_helpers.pandas_functions import processing
from cml_conversion_helpers.pandas_functions import dimension_cohorts
from cml_schemas import pandas_schemas

from msds_monthly_to_cml.utils import file_paths
from msds_monthly_to_cml.utils import logging_config


logger = logging.getLogger(__name__)

def main():
    
    # load config - here we load our project's parameters from the config file.
    config = file_paths.get_config("config.yaml")

    # configure logging - we can save information to log files which can be useful for debugging with logger.info()
    logging_config.configure_logging(config['log_dir'])
    logger.info(f"Configured logging with log folder: {config['log_dir']}.")
    logger.info(f"Logging the config settings:\n\n\t{config}\n")
    logger.info(f"Starting run at:\t{datetime.now().time()}")

    # Loading data from CSV as data frame
    df_maternity = pd.read_csv(config['path_to_source_data'])
    logger.info(f"Loaded source data from: {config['path_to_source_data']}.")

    # loop through the processing functions defined in the config
    logger.info(f"running functions defined in config...")
    for processing_func_config in config["processing_funcs"]:
        logger.info(f"   running {processing_func_config['name']}")
        processing_func = processing.PROCESSING_FUNC_REGISTRY[processing_func_config["name"]]
        df_maternity = processing_func(df_maternity, **processing_func_config["params"])
    logger.info(f"done!")

    # create the columns needed for the dimensions table
    df_maternity = dimension_cohorts.create_dimension_columns(
        df_maternity,
        "Dimension",
        "Measure",
        config["dimensions"],
        config["dimension_creation_exclusions"]
    )
    df_maternity = dimension_cohorts.create_dimension_type_col(
        df_maternity,
        config["dimensions"],
        "dimension_type_id"
    )
    df_maternity = dimension_cohorts.create_dimension_count_col(
        df_maternity,
        config["dimensions"],
        "dimension_count"
    )
    df_maternity = dimension_cohorts.create_md5_hash_col(
        df_maternity,
        config["dimensions"],
        "dimension_id"
    )
    df_maternity = processing.concat_cols(
        df_maternity,
        "datapoint_id",
        ["metric_id", "dimension_id", "reporting_grain", "location_id", "reporting_period_start_datetime"],
        prefix="",
        sep="_"
    )
    df_maternity = processing.concat_cols(df_maternity, "metric_dimension_id", ["metric_id", "dimension_id"], sep="_")
    logger.info(f"created the columns needed for the dimensions table.")

    # now df_maternity has all the columns needed for the dimensions and metric tables. the schemas module from the cml_schemas
    # package contains the schemas for each table. we can use the select_from_schema() function to select the columns
    # that belong to each schema, which leaves us with two new dataframes, one for each table.
    dimensions_schema = pandas_schemas.create_dimensions_schema(config["dimensions"])
    df_dimensions = pandas_schemas.select_from_schema(df_maternity, dimensions_schema)
    df_dimensions = df_dimensions.drop_duplicates()
    df_metric = pandas_schemas.select_from_schema(df_maternity, pandas_schemas.METRIC_SCHEMA)
    logger.info(f"created df_metric and df_dimensions")

    # Then we can save these to CSV
    logger.info(f"writing data to csv...")
    output_dir = Path(config['output_dir'])
    output_dir.mkdir(parents=True, exist_ok=True)
    df_metric.to_csv(output_dir / "metric.csv", index=False, date_format='%Y-%m-%d %H:%M:%S')
    df_dimensions.to_csv(output_dir / "dimensions.csv", index=False, date_format='%Y-%m-%d %H:%M:%S')
    logger.info(f"   done!")

if __name__ == "__main__":
    print(f"Running create_cml_tables script")
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    logger.info(f"Running time of create_cml_tables script: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.\n")
