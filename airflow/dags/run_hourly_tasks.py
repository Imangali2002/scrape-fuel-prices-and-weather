from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

from fetch.fuel import Fuel
from fetch.weather import Weather
from scrape.scrape_fuel import ScrapeFuel
from scrape.scrape_weather import ScrapeWeather
from scripts import clean_up

def call_func_fetch_fuel(**kwargs):
    Fuel(kwargs['country']).fetch_fuel()

def call_func_fetch_weather(**kwargs):
    Weather(kwargs['city_id'], kwargs['city']).fetch_weather()

def call_func_scrape_fuel(**kwargs):
    ScrapeFuel(kwargs['path']).scrape(kwargs['country'])

def call_func_scrape_weather(**kwargs):
    ScrapeWeather(kwargs['path']).scrape(kwargs['country'])


dag = DAG(
    dag_id="run_hourly_tasks",
    start_date=datetime(2022, 10, 20),
    schedule_interval="@hourly"
    )

run_fetch_kenyan_fuel = PythonOperator(
    dag=dag,
    task_id = 'run_fetch_kenyan_fuel',
    python_callable=call_func_fetch_fuel,
    op_kwargs={'country': 'Kenya'}
)

run_fetch_ugandan_fuel = PythonOperator(
    dag=dag,
    task_id = 'run_fetch_ugandan_fuel',
    python_callable=call_func_fetch_fuel,
    op_kwargs={'country': 'Uganda'}
)

run_fetch_kenyan_weather = PythonOperator(
    dag=dag,
    task_id = 'run_fetch_kenyan_weather',
    python_callable=call_func_fetch_weather,
    op_kwargs={'city_id': 251, 'city': 'nairobi'}
)

run_fetch_ugandan_weather = PythonOperator(
    dag=dag,
    task_id = 'run_fetch_ugandan_weather',
    python_callable=call_func_fetch_weather,
    op_kwargs={'city_id': 1328, 'city': 'kampala'}
)

run_scrape_kenyan_fuel = PythonOperator(
    dag=dag,
    task_id = 'run_scrape_kenyan_fuel',
    python_callable=call_func_scrape_fuel,
    op_kwargs={'path': './tmp/kenya_fuel_price.html', 'country': 'KE'},
)

run_scrape_ugandan_fuel = PythonOperator(
    dag=dag,
    task_id = 'run_scrape_ugandan_fuel',
    python_callable=call_func_scrape_fuel,
    op_kwargs={'path': './tmp/uganda_fuel_price.html', 'country': 'UG'},
)

run_scrape_kenyan_weather = PythonOperator(
    dag=dag,
    task_id = 'run_scrape_kenyan_weather',
    python_callable=call_func_scrape_weather,
    op_kwargs={'path': './tmp/nairobi_weather.html', 'country': 'KE'},
)

run_scrape_ugandan_weather = PythonOperator(
    dag=dag,
    task_id = 'run_scrape_ugandan_weather',
    python_callable=call_func_scrape_weather,
    op_kwargs={'path': './tmp/kampala_weather.html', 'country': 'UG'},
)


run_clean_up = PythonOperator(
    dag=dag,
    task_id = 'run_clean_up',
    python_callable=clean_up.clean_up
)

run_fetch_kenyan_fuel >> run_scrape_kenyan_fuel >> run_clean_up
run_fetch_ugandan_fuel >> run_scrape_ugandan_fuel >> run_clean_up
run_fetch_kenyan_weather >> run_scrape_kenyan_weather >> run_clean_up
run_fetch_ugandan_weather >> run_scrape_ugandan_weather >> run_clean_up
