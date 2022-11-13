CREATE SCHEMA data;
create user airflow_user with password 'airflow_user';
create user fastapi_user with password 'fastapi_user';
create user mlflow_user with password 'mlflow_user';
GRANT ALL PRIVILEGES ON DATABASE scraped_data TO airflow_user;
GRANT ALL PRIVILEGES ON DATABASE scraped_data TO fastapi_user;
GRANT ALL PRIVILEGES ON DATABASE scraped_data TO mlflow_user;
GRANT ALL PRIVILEGES on SCHEMA data to fastapi_user;
GRANT ALL PRIVILEGES on SCHEMA public to airflow_user;