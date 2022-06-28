import datetime
from datetime import timedelta
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.types import DagRunType

from astronomer.providers.snowflake.operators.snowflake_sql_api import (
    SnowflakeSqlApiOperatorAsync,
)
from astronomer.providers.snowflake.triggers.snowflake_sql_api_trigger import (
    SnowflakeSqlApiTrigger,
)

TASK_ID = "snowflake_sql_api"
CONN_ID = "my_snowflake_conn"
POLLING_PERIOD_SECONDS = 1.0
LIFETIME = timedelta(minutes=59)
RENEWAL_DELTA = timedelta(minutes=54)
SQL_MULTIPLE_STMTS = (
    "create or replace table user_test (i int); insert into user_test (i) "
    "values (200); insert into user_test (i) values (300); select i from user_test order by i;"
)

SINGLE_STMT = "select i from user_test order by i;"


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


def create_context(task):
    dag = DAG(dag_id="dag")
    execution_date = datetime.datetime(2022, 1, 1, 0, 0, 0)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=execution_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
    }


@pytest.mark.parametrize("mock_sql, statement_count", [(SQL_MULTIPLE_STMTS, 4), (SINGLE_STMT, 1)])
@mock.patch("astronomer.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.execute_query")
def test_snowflake_sql_api_execute_operator_async(mock_db_hook, mock_sql, statement_count):
    """
    Asserts that a task is deferred and an SnowflakeTrigger will be fired
    when the SnowflakeOperatorAsync is executed.
    """
    operator = SnowflakeSqlApiOperatorAsync(
        task_id=TASK_ID,
        snowflake_conn_id=CONN_ID,
        sql=mock_sql,
        statement_count=statement_count,
    )

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(create_context(operator))

    assert isinstance(exc.value.trigger, SnowflakeSqlApiTrigger), "Trigger is not a SnowflakeSqlApiTrigger"


def test_snowflake_sql_api_execute_complete_failure():
    """Test SnowflakeSqlApiOperatorAsync raise AirflowException of error event"""

    operator = SnowflakeSqlApiOperatorAsync(
        task_id=TASK_ID,
        snowflake_conn_id=CONN_ID,
        sql=SQL_MULTIPLE_STMTS,
        statement_count=4,
    )
    with pytest.raises(AirflowException):
        operator.execute_complete(
            context=None,
            event={"status": "error", "message": "Test failure message", "type": "FAILED_WITH_ERROR"},
        )


@pytest.mark.parametrize(
    "mock_event",
    [
        None,
        ({"status": "success", "statement_query_ids": ["uuid", "uuid"]}),
    ],
)
@mock.patch(
    "astronomer.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.check_query_output"
)
def test_snowflake_sql_api_execute_complete(mock_conn, mock_event):
    """Tests execute_complete assert with successful message"""

    operator = SnowflakeSqlApiOperatorAsync(
        task_id=TASK_ID,
        snowflake_conn_id=CONN_ID,
        sql=SQL_MULTIPLE_STMTS,
        statement_count=4,
    )

    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(context=None, event=mock_event)
    mock_log_info.assert_called_with("%s completed successfully.", TASK_ID)
