import pandas as pd
import requests
import logging
import os
from config import ConfigLoader
import json
import argparse
import datetime


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
    
class Extract:
    def __init__(self, pipeline_env_file, docker_env_file):
        self.config = ConfigLoader(pipeline_env_file, docker_env_file)
        
    def fetch_data(self, date:str):
        api_url = self.config.get('API_URL')

        readings = []
        stations = []

        pagination_token = None
        next_page = True
        count = 1
        try:
            while next_page:
                params = {
                    'date': date,
                    'paginationToken': pagination_token
                    }
                
                #call API for a specified date
                response = requests.get(api_url, params=params)
                if response.status_code == 200:
                    data = response.json()

                    logger.info(f"Data on page {str(count)} fetched successfully")

                    stations.extend(data['data']['stations'])
                    readings.extend(data['data']['readings'])

                    if 'paginationToken' not in data['data']:
                        next_page = False

                    else:
                        pagination_token = data['data']['paginationToken']
                
                else:
                    logger.error(f"Failed to fetch data. Received status code {response.status_code}")
                    return None
                
                count += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred when calling the API: {e}")
            return None
        
        return stations, readings
    
    def save_raw_file(self, data, type, date:str):
        raw_data_file_path = os.path.join(self.config.get('DATA_FILE_PATH'), 'raw')

        file_path = os.path.join(raw_data_file_path, type)

        if not os.path.exists(file_path):
            os.makedirs(file_path)

        try:
            with open(os.path.join(file_path, f"{date}.json"), 'w') as file:
                json.dump(data, file, indent=4)
            logger.info(f"Data saved to {os.path.join(file_path, f'{date}.json')}")
            
        except Exception as e:
            logger.error(f"Error saving data to file: {e}")
            

    def extract_and_process_data(self, date:str):
        try:
            stations, readings = self.fetch_data(date)

            if stations is None:
                logger.error("No stations data found")
                raise ValueError("No stations data found")
            
            else:
                self.save_raw_file(stations, type='stations', date=date)
                logger.info("Raw stations data saved")

            if readings is None:
                logger.error("No readings data found")
                raise ValueError("No readings data found")

            
            else:
                self.save_raw_file(readings, type='readings', date=date)
                logger.info("Raw readings data saved")

            return True
            
        except ValueError as ve:
            # This block handles expected errors (e.g., missing data)
            logger.error(f"Data extraction error: {ve}")
            return False

        except Exception as e:
            # Catch any other unexpected errors
            logger.error(f"An unexpected error occurred during data extraction: {e}")
            return False
    

if __name__ == '__main__':
    set_working_directory_to_script()

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', type=str, default=get_yesterday_date(), help='Date')

    args = parser.parse_args()

    process_date = args.date

    extract = Extract(pipeline_env_file='.env', docker_env_file='../docker/.env')
    data = extract.extract_and_process_data(process_date)

    if data:
        logger.info("Data extracted successfully")
    
    else:
        logger.error("Failed to extract data")