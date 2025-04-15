import pandas as pd
import logging
import os
import datetime
from extract import Extract
from transform import Transform
from load import Load
import argparse

# Configure the logger
logging.basicConfig(
    level=logging.DEBUG,  # Log everything from DEBUG level and above
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        logging.FileHandler('app.log')  # Also log to a file
    ]
)

logger = logging.getLogger()

def set_working_directory_to_script():
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.realpath(__file__))
    
    # Change the current working directory to the script's directory
    os.chdir(script_dir)
    logger.info(f"Working directory set to: {os.getcwd()}")

def get_yesterday_date():
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')  # Returns date in 'YYYY-MM-DD' form

class Pipeline:
    def __init__(self, pipeline_env_file, docker_env_file):
        self.extract = Extract(pipeline_env_file, docker_env_file)
        self.transform = Transform(pipeline_env_file, docker_env_file)
        self.load = Load(pipeline_env_file, docker_env_file)

    def run(self, date):
        """
        Executes the full ETL pipeline for a specific date.

        Args:
            date (str): The target date in 'YYYY-MM-DD' format.
        """
        try:
            logger.info("Starting ETL Pipeline")

            #Extract data from API and save file in server
            self.extract.extract_and_process_data(date)

            #Transform data
            stations_df, readings_df = self.transform.transform_data(date)

            #load data
            self.load.process_and_load_data(stations_df=stations_df, readings_df=readings_df)

            logger.info('ETL pipeline completed successfully')
            

        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise

if __name__ == '__main__':

    set_working_directory_to_script()

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', type=str, default=get_yesterday_date(), help='Date')

    args = parser.parse_args()
    process_date = args.date

    logger.info(f"Running ETL pipeline for date {process_date}")

    pipeline = Pipeline(pipeline_env_file='.env', docker_env_file='../docker/.env')
    pipeline.run(process_date)