import logging
import pandas as pd
import os
import psycopg2 
import datetime
import argparse
from config import ConfigLoader
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
from transform import Transform

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

class Load:
    def __init__(self, pipeline_env_file, docker_env_file):
        self.config = ConfigLoader(pipeline_env_file, docker_env_file)
        self.engine = self.connect_database()
        self.RAW_STATIONS_TABLE = 'raw_stations'
        self.STATIONS_TABLE = 'stations'
        self.RAW_READINGS_TABLE = 'raw_readings'
        self.READINGS_TABLE = 'readings'
        self.ALLOWED_RAW_TABLES = ['raw_stations', 'raw_readings']
    
    def connect_database(self):
        """
        Connects to postgresSQL Database

        Args:
            

        Raises:
            Exception: If fails to connect to database.

        Returns:
            engine: connection to Postgres
        """

        try:
            conn_string = f"postgresql+psycopg2://{self.config.get('POSTGRES_USER')}:{self.config.get('POSTGRES_PASSWORD')}@" \
                          f"{self.config.get('POSTGRES_HOST')}:5432/{self.config.get('POSTGRES_DB')}"
            
            # Create the SQLAlchemy engine
            engine = create_engine(conn_string)
            logger.info("Successfully connected to the database.")
            return engine
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            raise
    
    def truncate_raw(self, raw_table_name):
        """
        Truncates all data from the specified raw table.

        Args:
            raw_table_name (str): The name of the raw table to truncate.

        Raises:
            ValueError: If the table name is not in the allowed list.
            Exception: If the database operation fails.
        """

        sql = f"TRUNCATE TABLE {raw_table_name};"

        try:
            with self.engine.begin() as con:  #begin -> auto commits on success and rolls back on error
                if raw_table_name not in self.ALLOWED_RAW_TABLES:
                    raise ValueError (f"Invalid table name: {raw_table_name}")
                con.execute(text(sql))
            logger.info(f"Successfully truncated table {raw_table_name}.")
        except Exception as e:
            logger.error(f"Error truncating table {raw_table_name}: {e}")
            raise

    def load_df_to_postgres(self, df, raw_table_name):
        """
        Loads DF to database

        Args:
            dataframe (df): Data to insert
            raw_table_name: table which the data is to be inserted to

        Raises:
            Exception: If fails to connect to database.
        """
        try:
            df.to_sql(raw_table_name, con=self.engine, if_exists='append', index=False)
            logger.info(f"Data loaded to {raw_table_name} successfully")
        except Exception as e:
            logger.error(f"Error loading data to {raw_table_name}: {e}")
            raise
    
    def merge_stations(self):
        """
        Upsert raw_stations data to stations table

        Args:
            

        Raises:
            Exception: If SQL statement failes to execute.
        """

        sql = '''
            MERGE INTO stations AS target
            USING raw_stations AS source
            ON target.id = source.id
            AND target.deviceid = source.deviceid
            WHEN MATCHED THEN
                UPDATE SET 
                name = source.name,
                latitude = source.latitude,
                longitude = source.longitude
            WHEN NOT MATCHED THEN
                INSERT (id, deviceid, name, latitude, longitude)
                VALUES (source.id, source.deviceid, source.name, source.latitude, source.longitude)
                ;
            '''

        try:
            with self.engine.begin() as con:
                result = con.execute(text(sql))
            logger.info(f"Merge into stations successfully")
        except Exception as e:
            logger.error(f"Error merging into stations: {e}")
            raise

    def merge_readings(self):
        """
        Upsert raw_readings data to readings table

        Args:
            

        Raises:
            Exception: If SQL statement failes to execute.
        """

        sql = '''
            MERGE INTO readings AS target
            USING raw_readings AS source
            ON target.timestamp = source.timestamp
            AND target.stationid = source.stationid
            WHEN MATCHED THEN
                UPDATE SET 
                value = source.value
            WHEN NOT MATCHED THEN
                INSERT (timestamp, stationid, value)
                VALUES (source.timestamp, source.stationid, source.value)
                ;
            '''

        try:
            with self.engine.begin() as con:
                con.execute(text(sql))
            logger.info(f"Merge into readings successfully")
        except Exception as e:
            logger.error(f"Error merging into readings: {e}")
            raise

        return None
    
    def close_connection(self):
        """
        Close postgresSQL database connection

        Args:
            

        Raises:
            Exception: If fails to close connection.
        """
        try:
            self.engine.dispose()
            logger.info("Database connection closed.")
        except Exception as e:
            logger.error(f"Error closing the database connection: {e}")
            raise
    
    def process_and_load_data(self, stations_df, readings_df):
        """
        Main functions that process and load data into database

        Args:
            stations_df (df): From transform.py, stations data to load to db
            readings_df (df): From transform.py, readings data to load to db

        Raises:
            Exception: If any steps fail.
        """

        try:
            #Truncate raw table first
            self.truncate_raw(self.RAW_STATIONS_TABLE)
            self.truncate_raw(self.RAW_READINGS_TABLE)

            #Load to raw tables
            self.load_df_to_postgres(stations_df, self.RAW_STATIONS_TABLE)
            self.load_df_to_postgres(readings_df, self.RAW_READINGS_TABLE)

            #Merge to final table
            self.merge_stations()
            self.merge_readings()

            #close connection
            self.close_connection()

            logger.info("Data successfully loaded")
        except Exception as e:
            logger.error(f"Error when loading data: {e}")
            raise

if __name__ == '__main__':
    set_working_directory_to_script()

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', type=str, default=get_yesterday_date(), help='Date')

    args = parser.parse_args()

    process_date = args.date
    load = Load(pipeline_env_file='.env', docker_env_file='../docker/.env')

    # Step 1: Transform
    transform = Transform(pipeline_env_file='.env', docker_env_file='../docker/.env')
    stations_df, readings_df = transform.transform_data(date=process_date)

    load.process_and_load_data(stations_df, readings_df)

