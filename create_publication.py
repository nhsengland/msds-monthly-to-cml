import logging
import timeit 
import yaml
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

logger = logging.getLogger(__name__)

def main():
    
    # load config, here we load our project's parameters from the config.toml file
    # config = file_paths.get_config()
    with open('config.yaml', 'r') as file:
       config = yaml.safe_load(file)

    # create spark session
    spark = spark_utils.create_spark_session(config['project_name'])

    # Loading data from CSV as spark data frame
    df_maternity = reading_data.load_csv_into_spark_data_frame(spark, config['path_to_maternity_data'])

    for processing_func_config in config["processing_funcs"]:
        processing_func = processing.PROCESSING_FUNC_REGISTRY[processing_func_config["name"]]
        df_maternity = processing_func(df_maternity, **processing_func_config["params"])


    df_maternity = dimension_cohorts.create_dimension_table(
        df_maternity,
        config["dimensions"]
    )


    # mbrrace_groups = [
    #     "GROUP 1. LEVEL 3 NEONATAL INTENSIVE CARE UNIT (NICU) AND NEONATAL SURGERY",
    #     "GROUP 2. LEVEL 3 NICU",
    #     "GROUP 3. 4,000 OR MORE BIRTHS PER ANNUM AT 24 WEEKS OR LATER",
    #     "GROUP 4. 2,000 - 3,999 BIRTHS PER ANNUM AT 24 WEEKS OR LATER",
    #     "GROUP 5. UNDER 2,000 BIRTHS PER ANNUM AT 24 WEEKS OR LATER",
    # ]
    # df_maternity = processing.move_attributes_to_new_dimension(
    #     df_maternity,
    #     'Dimension',
    #     'england',
    #     'MBRRACE Grouping',
    #     'all_mbrrace_groupings',
    #     mbrrace_groups
    # )
    

    print(df_maternity.columns)

    output_name = "output.csv"
    write_csv.save_spark_dataframe_as_csv(df_maternity, output_name)
    logger.info(f"saved output df {output_name} as csv")
    write_csv.rename_csv_output(output_name)
    logger.info(f"renamed {output_name} file")
    
    # stop the spark session
    spark.stop()
        

if __name__ == "__main__":
    print(f"Running create_publication script")
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    logger.info(f"Running time of create_publication script: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.\n")
