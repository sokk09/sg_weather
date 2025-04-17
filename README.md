# Singapore Rainfall Data Extraction

## Project Overview
This project extracts rainfall data for Singapore from [Singapore's open data portal](data.gov.sg). The goal of the project is to familiarize myself with key data engineering technologies such as Docker, PostgreSQL, and Apache Airflow.

### Architecture
<img width="1451" alt="image" src="https://github.com/user-attachments/assets/5d6d21b2-bb3b-4da8-8ce7-7c6e382a1541" />

This project uses the following technologies:
- Docker: To containerize the services (PostgresSQL and Airflow)
- PostgresSQl: For storing the data
- Apache Airflow: For orchestrating the ETL pipeline
- Python: For processing the data

### Project Workflow

#### 1. Extract

The data is extracted from the open data portal of Singapore using Python's requests library and APIs.

#### 2. Transform

The extracted raw data is processed and transformed using Python and Pandas. This step involves:
* Cleaning the data
* Converting timestamp
* Structuring the data from json to dataframe for loading into the database

#### 3. Load
The transformed data is loaded into a PostgresSQL database. Data is first loaded to a staging table then upserted to a final table.

### Future Improvements
1. Automate data quality
    - Automate data quality checks and add testing between steps to ensure data quality
2. Improve pipeline monitoring
    - Implement notification in case of failure
3. Integrate with cloud platforms
    - Deploy to cloud platform like AWS or GCP for scalability

### Resources

1. [Postgres Docker Setup](https://www.docker.com/blog/how-to-use-the-postgres-docker-official-image/)
2. [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
3. [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
4. [Creating DAG](https://www.geeksforgeeks.org/how-to-create-first-dag-in-airflow/)
