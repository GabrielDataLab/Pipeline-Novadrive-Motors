from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024,1,1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retries_delay": timedelta(minutes=5)
}

@dag(
    dag_id = "postgres_to_snowflake",
    default_args=default_args,
    description="Load data incrementally from Postgres to Snowflake",
    schedule=None,
    catchup=False
)
def postgres_to_snowflake_etl():
    table_names = ["veiculos", "estados", 
                   "cidades", "concessionarias", 
                   "vendedores", "clientes", "vendas"]
    for i in table_names:
        @task(task_id=f'get_max_id_{i}')
        def get_max_primary_key(i:str):
            with SnowflakeHook(snowflake_conn_id="snowflake").get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT MAX(id_{i}) FROM {i}")
                    max_id = cur.fetchone()[0]
                    return max_id if max_id is not None else 0
    
        @task(task_id=f"load_data_{i}")
        def load_incremetal_data(i:str, max_id):
            with PostgresHook(postgres_conn_id="postgres").get_conn() as pg_conn:
                with pg_conn.cursor() as pg_cur:
                    primary_key = f"id_{i}"
                    pg_cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{i}'")
                    columns = [row[0] for row in pg_cur.fetchall()]
                    columns_list_str = ','.join(columns)
                    placeholders = ','.join(["%s"] * len(columns))

                    pg_cur.execute(f"SELECT {columns_list_str} FROM {i} WHERE {primary_key} > {max_id}")
                    rows = pg_cur.fetchall()

                    with SnowflakeHook(snowflake_conn_id="snowflake").get_conn() as sf_conn:
                        with sf_conn.cursor() as sf_cur:
                            insert_query = f"INSERT INTO {i} ({columns_list_str}) VALUES ({placeholders})"
                            for row in rows:
                              sf_cur.execute(insert_query,row)
        max_id = get_max_primary_key(i)
        load_incremetal_data(i, max_id)

postgres_to_snowflake_etl_dag = postgres_to_snowflake_etl()

