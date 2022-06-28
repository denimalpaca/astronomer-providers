import asyncio
from datetime import timedelta
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.snowflake.triggers.snowflake_sql_api_trigger import (
    SnowflakeSqlApiTrigger,
)

TASK_ID = "snowflake_sql_api"
POLLING_INTERVAL = 1.0
LIFETIME = timedelta(minutes=59)
RENEWAL_DELTA = timedelta(minutes=54)


def test_snowflake_sql_trigger_serialization():
    """
    Asserts that the SnowflakeSqlApiTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = SnowflakeSqlApiTrigger(
        task_id=TASK_ID,
        poll_interval=POLLING_INTERVAL,
        query_ids=[],
        snowflake_conn_id="test_conn",
        token_life_time=LIFETIME,
        token_renewal_delta=RENEWAL_DELTA,
    )
    classpath, kwargs = trigger.serialize()
    assert (
        classpath
        == "astronomer.providers.snowflake.triggers.snowflake_sql_api_trigger.SnowflakeSqlApiTrigger"
    )
    assert kwargs == {
        "task_id": TASK_ID,
        "poll_interval": 1.0,
        "query_ids": [],
        "snowflake_conn_id": "test_conn",
        "token_life_time": LIFETIME,
        "token_renewal_delta": RENEWAL_DELTA,
    }


@pytest.mark.asyncio
@mock.patch(
    "astronomer.providers.snowflake.triggers.snowflake_sql_api_trigger.SnowflakeSqlApiTrigger.is_still_running"
)
@mock.patch(
    "astronomer.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.get_sql_api_query_status"
)
async def test_snowflake_sql_trigger_running(mock_get_sql_api_query_status, mock_is_still_running):
    """Tests that the SnowflakeSqlApiTrigger in running by mocking is_still_running to true"""
    mock_is_still_running.return_value = True
    trigger = SnowflakeSqlApiTrigger(
        task_id=TASK_ID,
        poll_interval=POLLING_INTERVAL,
        query_ids=["uuid"],
        snowflake_conn_id="test_conn",
        token_life_time=LIFETIME,
        token_renewal_delta=RENEWAL_DELTA,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch(
    "astronomer.providers.snowflake.triggers.snowflake_sql_api_trigger.SnowflakeSqlApiTrigger.is_still_running"
)
@mock.patch(
    "astronomer.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.get_sql_api_query_status"
)
async def test_snowflake_sql_trigger_completed(mock_get_sql_api_query_status, mock_is_still_running):
    """
    Test SnowflakeSqlApiTrigger run method with success status and mock the get_sql_api_query_status result
    and  is_still_running to False.
    """
    mock_is_still_running.return_value = False
    statement_query_ids = ["uuid", "uuid1"]
    mock_get_sql_api_query_status.return_value = {
        "message": "Statement executed successfully.",
        "status": "success",
        "statement_handles": statement_query_ids,
    }
    trigger = SnowflakeSqlApiTrigger(
        task_id=TASK_ID,
        poll_interval=POLLING_INTERVAL,
        query_ids=["uuid"],
        snowflake_conn_id="test_conn",
        token_life_time=LIFETIME,
        token_renewal_delta=RENEWAL_DELTA,
    )
    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "success", "statement_query_ids": statement_query_ids}) == actual


@pytest.mark.asyncio
@mock.patch(
    "astronomer.providers.snowflake.triggers.snowflake_sql_api_trigger.SnowflakeSqlApiTrigger.is_still_running"
)
@mock.patch(
    "astronomer.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.get_sql_api_query_status"
)
async def test_snowflake_sql_trigger_failure_status(mock_get_sql_api_query_status, mock_is_still_running):
    """Test SnowflakeSqlApiTrigger task is executed and triggered with failure status."""
    mock_is_still_running.return_value = False
    mock_response = {
        "status": "error",
        "message": "An error occurred when executing the statement. Check "
        "the error code and error message for details",
    }
    mock_get_sql_api_query_status.return_value = mock_response
    trigger = SnowflakeSqlApiTrigger(
        task_id=TASK_ID,
        poll_interval=POLLING_INTERVAL,
        query_ids=["uuid"],
        snowflake_conn_id="test_conn",
        token_life_time=LIFETIME,
        token_renewal_delta=RENEWAL_DELTA,
    )
    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent(mock_response) == actual


@pytest.mark.asyncio
@mock.patch(
    "astronomer.providers.snowflake.triggers.snowflake_sql_api_trigger.SnowflakeSqlApiTrigger.is_still_running"
)
@mock.patch(
    "astronomer.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.get_sql_api_query_status"
)
async def test_snowflake_sql_trigger_exception(mock_get_sql_api_query_status, mock_is_still_running):
    """Tests the SnowflakeSqlApiTrigger does not fire if there is an exception."""
    mock_is_still_running.return_value = False
    mock_get_sql_api_query_status.side_effect = Exception("Test exception")

    trigger = SnowflakeSqlApiTrigger(
        task_id=TASK_ID,
        poll_interval=POLLING_INTERVAL,
        query_ids=["uuid"],
        snowflake_conn_id="test_conn",
        token_life_time=LIFETIME,
        token_renewal_delta=RENEWAL_DELTA,
    )

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_query_id, mock_response, expected_status",
    [
        (["uuid"], {"status": "success"}, False),
        (["uuid"], {"status": "error"}, False),
        (["uuid"], {"status": "running"}, True),
    ],
)
@mock.patch(
    "astronomer.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.get_sql_api_query_status"
)
async def test_snowflake_sql_trigger_is_still_running(
    mock_get_sql_api_query_status, mock_query_id, mock_response, expected_status
):
    mock_get_sql_api_query_status.return_value = mock_response
    trigger = SnowflakeSqlApiTrigger(
        task_id=TASK_ID,
        poll_interval=POLLING_INTERVAL,
        query_ids=mock_query_id,
        snowflake_conn_id="test_conn",
        token_life_time=LIFETIME,
        token_renewal_delta=RENEWAL_DELTA,
    )
    response = await trigger.is_still_running(mock_query_id)
    assert response == expected_status
