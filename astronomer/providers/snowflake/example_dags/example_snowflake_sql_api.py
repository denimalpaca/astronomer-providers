"""Example use of SnowflakeAsync related providers."""

import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.snowflake.operators.snowflake_sql_api import SnowflakeSQLOperatorAsync

SNOWFLAKE_CONN_ID = os.getenv("ASTRO_SNOWFLAKE_CONN_ID", "snowflake_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

# SQL commands
SQL_MULTIPLE_STMTS = "create or replace table user_test (i int); insert into user_test (i) " \
                     "values (200); insert into user_test (i) values (300); select i from user_test order by i;"
SINGLE_STMT = "select i from user_test order by i;"

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "snowflake_conn_id": SNOWFLAKE_CONN_ID,
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_snowflake_sql_api",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    tags=["example", "async", "snowflake"],
    catchup=False,
) as dag:
    # [START howto_operator_snowflake_op_sql_multiple_stmt]
    snowflake_op_sql_multiple_stmt = SnowflakeSQLOperatorAsync(
        task_id="snowflake_op_sql_multiple_stmt",
        dag=dag,
        sql=SQL_MULTIPLE_STMTS,
        statement_count=4,
    )
    # [END howto_operator_snowflake_op_sql_multiple_stmt]

    # [START howto_operator_snowflake_single_sql_stmt]
    snowflake_single_sql_stmt = SnowflakeSQLOperatorAsync(
        task_id="snowflake_single_sql_stmt",
        dag=dag,
        sql=SINGLE_STMT,
        statement_count=1,
    )
    # [END howto_operator_snowflake_single_sql_stmt]

    (
            snowflake_op_sql_multiple_stmt >>
            snowflake_single_sql_stmt

    )