import pandas as pd
import logging
import os
import datetime
from config import ConfigLoader
import json
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

class Transform:
    def __init__(self, pipeline_env_file, docker_env_file):
        self.config = ConfigLoader(pipeline_env_file, docker_env_file)

    def read_data_files(self, type, date:str):

        raw_data_file_path = os.path.join(self.config.get('DATA_FILE_PATH'), 'raw')
        file_path = os.path.join(raw_data_file_path, type)

        try:
            with open(os.path.join(file_path, f'{date}.json'), 'r') as json_file:
                json_data = json.load(json_file)
                logger.info(f"Loaded {type} {date} file from {file_path}")
        except Exception as e:
            logger.error(f"Error reading {os.path.join(file_path, f'{date}.json')}: {e}")
            raise KeyError(f"Error reading {os.path.join(file_path, f'{date}.json')}: {e}")
        
        return json_data
    
    def load_station_data_to_df(self, date:str):
        station_json= self.read_data_files(type='stations',date=date)

        df = pd.json_normalize(
            data=station_json,
            errors='raise'
        )
        
        logger.info("Station data loaded to df")
        return df

    def transform_station_data(self, date:str):

        df = self.load_station_data_to_df(date=date)

        # Convert all column names to lowercase
        df.columns = df.columns.str.lower()

        #validate the datatype first
        self.validate_station_data(df)
        logger.info("All stations datatype match")

        #Change column name
        df.rename(columns={'location.latitude': 'latitude', 'location.longitude': 'longitude'}, inplace=True)
        logger.info("Column name change")

        # Convert all column names to lowercase
        df.columns = df.columns.str.lower()

        #remove duplicates
        df = df.drop_duplicates()

        return df
    
    def validate_station_data(self, df):
        assert df['id'].dtype == 'object', "id column should be of type object"
        assert df['deviceid'].dtype == 'object', "deviceId column should be of type object"
        assert df['name'].dtype == 'object', "name column should be of type object"
        assert df['location.latitude'].dtype == 'float', "latitude column should be of type string"
        assert df['location.longitude'].dtype == 'float', "longitude column should be of type string"

    def load_readings_data_to_df(self, date:str):
        readings_json= self.read_data_files(type='readings',date=date)

        df = pd.json_normalize(
            data=readings_json,
            meta='timestamp',
            record_path='data',
            errors='raise'
        )

        logger.info(f"Loaded readings data to df")


        return df

    def transform_readings_data(self, date:str):

        df = self.load_readings_data_to_df(date=date)

        # Convert all column names to lowercase
        df.columns = df.columns.str.lower()

        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
        logger.info("Changed timestamp datatype")

        #validate the datatype first
        self.validate_readings_data(df)
        logger.info("All readings datatype match")

        return df
    
    def validate_readings_data(self, df):
        assert df['stationid'].dtype == 'object', "stationId column should be of type object"
        assert df['value'].dtype == 'float', "deviceId column should be of type float"
        assert pd.api.types.is_datetime64_any_dtype(df['timestamp']), "name column should be of datetime object"

    def transform_data(self,date):
        station_df = self.transform_station_data(date)
        readings_df = self.transform_readings_data(date)

        return station_df, readings_df

if __name__ == '__main__':
    set_working_directory_to_script()

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', type=str, default=get_yesterday_date(), help='Date')

    args = parser.parse_args()

    process_date = args.date
    transform = Transform(pipeline_env_file='.env', docker_env_file='../docker/.env')

    station_df, readings_df = transform.transform_data(date=process_date)

    print(station_df[station_df['id']== 'S219'])

