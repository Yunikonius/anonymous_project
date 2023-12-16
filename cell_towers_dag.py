import requests
import lzma
import logging
import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
import pendulum

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='/Users/yurisafonov/Downloads/CISM_Project/app.log')

logging.debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning message')
logging.error('This is an error message')
logging.critical('This is a critical message')

client = clickhouse_connect.get_client(host='localhost', port=8123)


def file_loader():
    url = 'https://datasets.clickhouse.com/cell_towers.csv.xz'
    response = requests.get(url)

    with open('/Users/yurisafonov/Downloads/CISM_Project/cell_towers.csv.xz', 'wb') as file:
        file.write(response.content)

    with lzma.open(file, 'rb') as input_file:
        uncompressed_data = input_file.read()

    with open('/Users/yurisafonov/Downloads/CISM_Project/cell_towers.csv', 'wb') as output_file:
        output_file.write(uncompressed_data)


def src_ddl():

    client.execute("""
delete from table src_cell_towers;
                   """)

    client.execute("""
create table if not exists src_cell_towers
(
	  radio varchar(255)
	, mcc integer
	, net integer
	, "area" integer
	, cell integer NOT NULL
	, unit integer
	, lon decimal(10,4)
	, lat decimal(10,4)
	, "range" integer
	, samples integer
	, changeable integer
	, created timestamp
	, updated timestamp
	, averageSignal integer
)
ENGINE = MergeTree()
ORDER BY cell;
               """)


def file_to_src():
    insert_file(
        client, 'src_cell_towers', '/Users/yurisafonov/Downloads/CISM_Project/cell_towers.csv',
        settings={'input_format_allow_errors_ratio': .2,
                  'input_format_allow_errors_num': 5}
    )


def stg_ddl():
    # механизм дедупликации через ReplacingMergeTree по атрибутам из order by
    # https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree
    client.command("""create table if not exists stg_cell_towers 
(
	  radio varchar(255)
	, mcc integer
	, net integer 
	, "area" integer
	, cell integer NOT NULL
	, unit integer
	, lon decimal(10,4)
	, lat decimal(10,4)
	, "range" integer
	, samples integer
	, changeable integer
	, created timestamp
	, updated timestamp
	, averageSignal integer
) 
ENGINE = ReplacingMergeTree 
PRIMARY KEY (cell, area, radio)
ORDER BY (cell, area, radio);""")


def src_to_stg():
    client.command("""
INSERT INTO stg_cell_towers 
(radio, mcc, net, area, cell, unit, lon, lat , "range", samples, changeable, created , updated, averageSignal)
SELECT 
  radio
, mcc
, net
, area
, cell
, unit
, lon
, lat 
, "range"
, samples
, changeable
, created 
, updated
, averageSignal
FROM 
(Select 
  sct.*
, ROW_NUMBER() OVER(PARTITION BY cell, radio ORDER BY updated desc) as rn 
From src_cell_towers sct) t
Where t.rn = 1;
               """)


def mart_create():
    # сделал через представление, как вариант можно через мат представление
    client.command("""
CREATE view if not exists cdm_cell_towers AS
(Select distinct 
  area
--, count(distinct cell)
from stg_cell_towers sc
where mcc = 250
and area not in (select distinct area from stg_cell_towers t where radio = 'LTE')
group by area
having count(distinct cell) > 200);
               """)


@dag(
    dag_id='cell_towers_loader',
    schedule_interval="0 12 1 * *",
    start_date=pendulum.parse("2023-12-15"),
    catchup=False
)
def cell_loader_dag():

    file_loader_task = PythonOperator(
        task_id='file_loader_task',
        python_callable=file_loader()
    )

    src_ddl_task = PythonOperator(
        task_id='src_ddl_task',
        python_callable=src_ddl()
    )

    file_to_src_task = PythonOperator(
        task_id='file_to_src_task',
        python_callable=file_to_src()
    )

    stg_ddl_task = PythonOperator(
        task_id='stg_ddl_task',
        python_callable=stg_ddl()
    )

    src_to_stg_task = PythonOperator(
        task_id='src_to_stg_task',
        python_callable=src_to_stg()
    )

    mart_create_task = PythonOperator(
        task_id='mart_create_task',
        python_callable=mart_create()
    )

    (
        file_loader_task
        >> src_ddl_task
        >> file_to_src_task
        >> stg_ddl_task
        >> src_to_stg_task
        >> mart_create_task  # можно убрать после первичного форирования представления
    )


_ = cell_loader_dag()
