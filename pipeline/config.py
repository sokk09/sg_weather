import logging
from dotenv import load_dotenv
import os

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

class ConfigLoader:
    def __init__(self, pipeline_env_file, docker_env_file):
        self.config = {}
        self.load_config(pipeline_env_file, docker_env_file)

    def load_config(self, pipeline_env_file, docker_env_file):

        script_dir = os.path.dirname(os.path.realpath(__file__))  # Directory of this script
        pipeline_env_file = os.path.join(script_dir, pipeline_env_file)
        docker_env_file = os.path.join(script_dir, docker_env_file)

        if not load_dotenv(pipeline_env_file):
            logger.error(f"Failed to load pipeline env file {pipeline_env_file}")
            raise EnvironmentError(f"Failed to load pipeline env file {pipeline_env_file}")
        
        logger.info(f"{pipeline_env_file} successfully loaded")

        if not load_dotenv(docker_env_file):
            logger.error(f"Failed to load pipeline env file {docker_env_file}")
            raise EnvironmentError(f"Failed to load pipeline env file {docker_env_file}")
        
        logger.info(f"{docker_env_file} successfully loaded")

        self.config = {
            'API_URL': os.getenv('API_URL'),
            'POSTGRES_USER': os.getenv('POSTGRES_USER'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB'),
            'POSTGRES_HOST': os.getenv('POSTGRES_HOST'),
            'DATA_FILE_PATH':os.getenv('DATA_FILE_PATH'),
            }

        self.validate_config()

        return self.config
    
    def validate_config(self):
        required = ['API_URL', 'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB', 'DATA_FILE_PATH']
        for e in required:
            if not self.config.get(e):
                logger.error(f"{e} not found in config")
                raise ValueError(f"{e} not found in config")
        
        logger.info("All environment variables loaded")
    
    def get(self, key):
        return self.config.get(key)

# if __name__ == '__main__':
    # config = ConfigLoader(pipeline_env_file='.env', docker_env_file='../docker/.env')
    # api_url = config.get('API_URL')
    # print(api_url)
