import asyncio
from datetime import timedelta
from typing import Any, AsyncIterator, Dict, List, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSQLAPIHookAsync


class SnowflakeSQLAPITrigger(BaseTrigger):
    """
    SnowflakeSQLAPI Trigger inherits from the BaseTrigger,it is fired as
    deferred class with params to run the task in trigger worker and
    fetch the status for the query ids passed

    :param task_id: Reference to task id of the Dag
    :param polling_period_seconds:  polling period in seconds to check for the status
    :param query_ids: List of Query ids to run and poll for the status
    :param snowflake_conn_id: Reference to Snowflake connection id
    """

    def __init__(
        self,
        task_id: str,
        polling_period_seconds: float,
        query_ids: List[str],
        snowflake_conn_id: str,
        token_life_time: timedelta,
        token_renewal_delta: timedelta,
    ):
        super().__init__()
        self.task_id = task_id
        self.polling_period_seconds = polling_period_seconds
        self.query_ids = query_ids
        self.snowflake_conn_id = snowflake_conn_id
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes SnowflakeTrigger arguments and classpath."""
        return (
            "astronomer.providers.snowflake.triggers.snowflake_sql_api_trigger.SnowflakeSQLAPITrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "query_ids": self.query_ids,
                "snowflake_conn_id": self.snowflake_conn_id,
                "token_life_time": self.token_life_time,
                "token_renewal_delta": self.token_renewal_delta
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Makes a GET API request to snowflake with queri_id to get the status of the query
        by get_sql_api_query_status async function
        """
        hook = SnowflakeSQLAPIHookAsync(
            self.snowflake_conn_id,
            self.token_life_time,
            self.token_renewal_delta)
        try:
            statement_query_ids = []
            for query_id in self.query_ids:
                while await self.is_still_running(query_id):
                    await asyncio.sleep(1)
                statement_status = await hook.get_sql_api_query_status(query_id)
                if statement_status["status"] == "error":
                    yield TriggerEvent(statement_status)
                if statement_status["status"] == "success":
                    statement_query_ids.extend(statement_status["statement_handles"])
            yield TriggerEvent({"status": "success", "statement_query_ids": statement_query_ids})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    async def is_still_running(self, query_id: str) -> bool:
        """
        Async function to check whether the query statement submitted via SQL API is still
        running state and returns True if it is still running else
        return False
        """
        hook = SnowflakeSQLAPIHookAsync(
            self.snowflake_conn_id,
            self.token_life_time,
            self.token_renewal_delta)
        statement_status = await hook.get_sql_api_query_status(query_id)
        if statement_status["status"] in ["running"]:
            return True
        return False
