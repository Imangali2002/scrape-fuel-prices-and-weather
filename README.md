# scrape weather and fuel prices
In this project I used apache airflow to scrape website periodically, use beautiful soup to clean the data, use postgresql database to store the data and store the metadata for airflow, use plotly dash to create an analytics dashbaord.

## Technologies used
<img src="images/beautifulsoup.png" alt="Apache Airflow" width="200" height="100"/>&nbsp;&nbsp;<img src="images/AirflowLogo.png" alt="BeautifulSoup" width="200" height="100" />&nbsp;&nbsp;<img src="images/fastapi.png" alt="fast api" width="200" height="100" />

<img src="images/postgresql.png" alt="postgresql" width="200"  height="100" />&nbsp;&nbsp;<img src="images/dash.png" alt="dash" width="200" height="100" />&nbsp;&nbsp;<img src="images/alchemy.jpeg" alt="sqlalchemy" width="200" height="100" />

## Run project
#### 1. Clone Project
```
git clone https://github.com/Imangali2002/scrape-fuel-prices-and-weather.git
```
```
cd into scrape-fuel-prices-and-weather
```
#### 2. Run PostgreSQL with docker
```
docker run --name postgres_container -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=scraped_data -p 5423:5432 -v "$PWD"/pg_database:/var/lib/postgresql/data -d postgres
```
connect to the docker container bash
```
docker exec -it container_id bash
```
connect to psql command line
```
psql -U postgres -p 5432 -d scraped_data
```
for connect from main PC `psql -U postgres -h 127.0.0.1 -p 5423 -d scraped_data`

run below sql commands to create users and give privileges
```
CREATE SCHEMA data;
CREATE USER airflow_user WITH PASSWORD 'airflow_user';
CREATE USER fastapi_user WITH PASSWORD 'fastapi_user';
CREATE USER mlflow_user WITH PASSWORD 'mlflow_user';
GRANT ALL PRIVILEGES ON DATABASE scraped_data TO airflow_user;
GRANT ALL PRIVILEGES ON DATABASE scraped_data TO fastapi_user;
GRANT ALL PRIVILEGES ON DATABASE scraped_data TO mlflow_user;
GRANT ALL PRIVILEGES ON SCHEMA data to fastapi_user;
GRANT ALL PRIVILEGES ON SCHEMA public to airflow_user;
```

#### 3. Create enviroment and install requirements
```
python -m venv venv
```
activate enviroment(Ubuntu)
```
source venv/bin/activate
```
for windows `venv\Scripts\activate`
```
pip install -r requirements.txt
```
```
pip install --upgrade pip
```
```
mkdir tmp
```
#### 4. Run FastAPI
activate enviroment and run below command
```
source venv/bin/activate
```
```
uvicorn api.main:app
```
#### 5. Run Airflow
```
source venv/bin/activate
```
```
export AIRFLOW_HOME="$PWD"/airflow
```
```
export PYTHONPATH="$PWD"
```
```
airflow init db
```
Open `airflow.cfg` and set `sql_alchemy_conn` with below options and set `load_examples` to False
```
sql_alchemy_conn = postgresql+psycopg2://airflow_user:airflow_user@127.0.0.1:5423/scraped_data
```
```
load_examples = False
```
create user
```
airflow users create --username airflow --firstname fname --lastname lname --role Admin --email airflow@domain.com --password airflow
```
start webserver
```
airflow webserver
```
open new terminal, activate enviroment and run scheduler
```
source venv/bin/activate
``` 
```
airflow scheduler
```

#### 6. Run Dash
```
source venv/bin/activate
```
start dash app
```
python dash/app.py
```


#### Open workspaces
FastAPI documentation - `http://0.0.0.0:8000/docs`
Airflow web app - `http://0.0.0.0:8080/`
Dash web app - `http://127.0.0.1:8050/`

<br>
Run Airflow `Dags` and after few minutes refresh dash app.