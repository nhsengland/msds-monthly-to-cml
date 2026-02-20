"""
Purpose of the script:  to provide an example of good practices when structuring a pipeline using PySpark

The script loads Python packages but also internal modules (e.g. modules.helpers, helpers script from the modules folder).
It then loads various configuration variables and a logger, for more info on see the RAP Community of Practice website:
https://nhsdigital.github.io/rap-community-of-practice/

Most of the code to carry out this configuration and setup is found in the utils folder.

Then, the main pipeline itself begins, which has three phases:

data_ingestion: 
    we download the artificial hes data, load it into a spark dataframe. Any other cleaning or preprocessing should
    happen at this stage
processing: 
    we process the data as needed, in this case we create some aggregate counts based on the hes data
data_exports: 
    finally we write our outputs to an appropriate file type (CSV)

Note that in the src folder, each of these phases has its own folder, to neatly organise the code used for each one.

"""

# this part imports our Python packages, pyspark functions, and our project's own modules
import logging
import timeit 
from datetime import datetime 

from pyspark.sql import functions as F

from src.utils import file_paths
from src.utils import logging_config
from src.utils import spark as spark_utils
from src.data_ingestion import get_data
from src.data_ingestion import reading_data
from src.processing import processing
from src.data_exports import write_csv

logger = logging.getLogger(__name__)

def main():
    
    # load config, here we load our project's parameters from the config.toml file
    config = file_paths.get_config() 

    # configure logging
    logging_config.configure_logging(config['log_dir'])
    logger.info(f"Configured logging with log folder: {config['log_dir']}.")
    logger.info(f"Logging the config settings:\n\n\t{config}\n")
    logger.info(f"Starting run at:\t{datetime.now().time()}")

    # create spark session
    spark = spark_utils.create_spark_session(config['project_name'])
    logger.info(f"created spark session with app name: {config['project_name']}")

    # Loading data from CSV as spark data frame
    df_maternity = reading_data.load_csv_into_spark_data_frame(spark, config['path_to_maternity_data'])

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
    
    dimension_cols = processing.get_dimensions_from_col(df_maternity, "Dimension")
    print(dimension_cols)


    # output_name = "output.csv"
    # write_csv.save_spark_dataframe_as_csv(df_maternity, output_nameget_dimensions_from_col(df, dimension_col_name))
    # logger.info(f"saved output df {output_name} as csv")
    # write_csv.rename_csv_output(output_name)
    # logger.info(f"renamed {output_name} file")
    
    # stop the spark session
    spark.stop()
        

if __name__ == "__main__":
    print(f"Running create_publication script")
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    logger.info(f"Running time of create_publication script: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.\n")
