import logging
import timeit
from datetime import datetime
from pathlib import Path
import os

import pandas as pd
from dotenv import load_dotenv

from cml_conversion_helpers.pandas_functions import processing
from cml_conversion_helpers.pandas_functions import dimension_cohorts
from cml_schemas import pandas_schemas

from msds_monthly_to_cml.data_ingestion import get_data
from msds_monthly_to_cml.utils import file_paths
from msds_monthly_to_cml.utils import logging_config
from msds_monthly_to_cml.utils import utils
from msds_monthly_to_cml.processing import msds_functions
from msds_monthly_to_cml.queries import reference_data


logger = logging.getLogger(__name__)

def main():
    
    # load config - here we load our project's parameters from the config file.
    logger.info(f"Getting ready to run pipeline....")
    config = file_paths.get_config("config.yaml")
    logger.info(f"  config loaded")
    load_dotenv()
    logger.info(f"  environment variables loaded")

    # configure logging - we can save information to log files which can be useful for debugging with logger.info()
    logging_config.configure_logging(config['log_dir'])
    logger.info(f"  configured logging with log folder: {config['log_dir']}.")
    logger.info(f"  logging the config settings:\n\n\t{config}\n")
    logger.info(f"  starting run at:\t{datetime.now().time()}")
    logger.info(f"  Ready!")

    metric_status = msds_functions.get_metric_status(config)
    logger.info(f"Metric status identified as:\t{metric_status}")


    # Loading data from CSV as data frame
    df_maternity = pd.read_csv(config['path_to_source_data'])
    logger.info(f"Loaded source data from: {config['path_to_source_data']}.")


    logger.info("running processing functions...")
    logger.info("  replacing null Org_Code values with Unknown")
    df_maternity["Org_Code"] = df_maternity["Org_Code"].fillna("null")

    logger.info("  running move_attributes_to_new_dimension")
    df_maternity = dimension_cohorts.move_attributes_to_new_dimension(
        df_maternity,
        source_col_name="Org_Code",
        source_col_fill_value="england",
        new_col_name="mbrrace_grouping",
        new_col_fill_value="no_mbrrace_grouping_filter",
        attributes_to_move=[
            "Group 1. Level 3 NICU & NS",
            "Group 2. Level 3 NICU",
            "Group 3. 4,000 or more",
            "Group 4. 2,000 - 3,999",
            "Group 5. Under 2,000"
        ]
    )

    logger.info("  running replace_ons_with_ods")
    conn = get_data.get_sql_connection(os.getenv("SERVER"), "DSS_CORPORATE")
    logger.info(f"    created connection to {os.getenv('SERVER')}")
    df_ons_to_ods_map = get_data.run_sql_query(reference_data.ons_to_ods_map, conn)
    ons_to_ods_map = dict(zip(df_ons_to_ods_map["Org_Code"], df_ons_to_ods_map["ods_code"]))
    df_maternity = processing.replace_col_values(
        df_maternity,
        col_name="Org_Code",
        value_mappings=ons_to_ods_map
    )

    logger.info("  running replace_col_values")
    df_maternity = processing.replace_col_values(
        df_maternity,
        col_name="Org_Code",
        value_mappings={"ALL": "england"}
    )

    logger.info("  running add_lit_col")
    df_maternity = processing.add_lit_col(
        df_maternity,
        col_name="additional_metric_values",
        col_value=None
    )

    logger.info("  adding locations to additional_metric_values")
    df_maternity["additional_metric_values"] = df_maternity.apply(
        lambda row: processing.add_json_key(
            cell=row["additional_metric_values"],
            key="Org_Level",
            value=row["Org_Level"],
        ),
        axis=1,
    )  

    logger.info("  running rename_cols")
    df_maternity = processing.rename_cols(
        df_maternity,
        col_name_mappings={
            "Org_Code": "location_id",
            "Org_Level": "location_type",
            "Final_value": "metric_value",
            "ReportingPeriodStartDate": "reporting_period_start_datetime",
            "ReportingPeriodEndDate": "last_record_timestamp"
        }
    )


    logger.info("  fixing null or unknown location_id values by concatenating location_type")
    df_maternity = msds_functions.fix_location_id_unknowns(df_maternity)


    logger.info("  running cast_date_col_to_timestamp")
    df_maternity = processing.cast_date_col_to_timestamp(
        df_maternity,
        col_name="reporting_period_start_datetime"
    )

    logger.info("  running cast_date_col_to_timestamp")
    df_maternity = processing.cast_date_col_to_timestamp(
        df_maternity,
        col_name="last_record_timestamp"
    )

    logger.info("  running add_lit_col")
    df_maternity = processing.add_lit_col(
        df_maternity,
        col_name="reporting_grain",
        col_value="monthly"
    )

    logger.info("  running create_uuid_col")
    df_maternity = processing.create_uuid_col(
        df_maternity,
        col_name="datapoint_id",
        length=32
    )

    logger.info("  running concat_cols")
    df_maternity = processing.concat_cols(
        df_maternity,
        new_col_name="metric_id",
        cols_to_concat=["Dimension", "Count_Of"],
        prefix="",
        sep="_",
        value_suffix=f"{metric_status}"
    )
    df_maternity["metric_id"] = df_maternity["metric_id"].str.replace(' ', '_')
    logger.info("  running replace_col_values")
    df_maternity = processing.replace_col_values(
        df_maternity,
        col_name="metric_id",
        value_mappings=config["metric_id_replacements"]
    )


    logger.info("  running add_lit_col")
    df_maternity = processing.add_lit_col(
        df_maternity,
        col_name="publication_datetime",
        col_value=config["publication_date"]
    )

    logger.info("  running cast_date_col_to_timestamp")
    df_maternity = processing.cast_date_col_to_timestamp(
        df_maternity,
        col_name="publication_datetime"
    )

    logger.info("  running add_lit_col")
    df_maternity = processing.add_lit_col(
        df_maternity,
        col_name="last_ingest_timestamp",
        col_value=config["last_ingest_timestamp"]
    )

    logger.info("  running cast_date_col_to_timestamp")
    df_maternity = processing.cast_date_col_to_timestamp(
        df_maternity,
        col_name="last_ingest_timestamp"
    )

    conn = get_data.get_sql_connection(os.getenv("SERVER"), "DSS_CORPORATE")
    logger.info(f"    created connection to {os.getenv('SERVER')}")
    logger.info(f"  adding location_types from reference data")
    df_org_code_to_type_map = get_data.run_sql_query(reference_data.org_code_to_type_map, conn)
    logger.info(f"  ran org_code_to_type_map SQL query on server and returned result")
    df_maternity = msds_functions.add_location_type_id_col(
        df_maternity,
        df_org_code_to_type_map,
        config["non_place_codes"]
    )
    logger.info(f"  added location_types")

    logger.info("  done!")

    logger.info("creating dimension cohorts..")
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
    df_maternity = processing.create_md5_hash_col_with_exceptions(
        df_maternity,
        config["dimensions"],
        "dimension_id",
        ["all_", "no_"]
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
    df_dimensions = msds_functions.filter_out_existing_dimensions(df_dimensions, config['output_dir'])
    df_metric = pandas_schemas.select_from_schema(df_maternity, pandas_schemas.METRIC_SCHEMA)
    logger.info(f"created df_metric and df_dimensions")

    
    # Creating generated timestamps
    generated_ts = datetime.now()
    df_metric = processing.add_lit_col(
        df_metric,
        col_name="generation_ts",
        col_value=generated_ts
    )
    df_metric = processing.concat_cols(df_metric, "datapoint_id_generation_ts", ["datapoint_id", "generation_ts"], sep="__")
    df_dimensions = processing.add_lit_col(
        df_dimensions,
        col_name="generation_ts",
        col_value=generated_ts
    )
    df_dimensions = processing.concat_cols(df_dimensions, "dimension_id_generation_ts", ["dimension_id", "generation_ts"], sep="__")
    logger.info(f"added generation_ts and pk")

    # Then we can save these to CSV
    logger.info(f"writing data to csv...")
    output_dir = Path(config['output_dir'])
    output_dir.mkdir(parents=True, exist_ok=True)
    reporting_period = utils.get_reporting_period_string(df_metric)
    filename_date_suffix = "__" + reporting_period + "__" + f"{metric_status}" + "__" + generated_ts.strftime("%Y-%m-%d--%H-%M-%S")
    logger.info(f"  saving metric data...")
    df_metric.to_csv(output_dir / f"metric_{filename_date_suffix}.csv", index=False, date_format='%Y-%m-%d %H:%M:%S')
    logger.info(f"  done!...")
    logger.info(f"  saving dimension data...")
    df_dimensions.to_csv(output_dir / f"dimensions_{filename_date_suffix}.csv", index=False, date_format='%Y-%m-%d %H:%M:%S')
    logger.info(f"  done!...")
    logger.info(f"All done!")

if __name__ == "__main__":
    print(f"Running create_cml_tables script")
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    logger.info(f"Running time of create_cml_tables script: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.\n")
