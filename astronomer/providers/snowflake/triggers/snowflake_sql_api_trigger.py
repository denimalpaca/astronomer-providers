import asyncio
from typing import Any, AsyncIterator, Dict, List, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSQLAPIHookAsync


class SnowflakeSQLAPITrigger(BaseTrigger):
    """
    Snowflake Trigger inherits from the BaseTrigger,it is fired as
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
    ):
        super().__init__()
        self.task_id = task_id
        self.polling_period_seconds = polling_period_seconds
        self.query_ids = query_ids
        self.snowflake_conn_id = snowflake_conn_id

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes SnowflakeTrigger arguments and classpath."""
        return (
            "astronomer.providers.snowflake.triggers.snowflake_trigger.SnowflakeTrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "query_ids": self.query_ids,
                "snowflake_conn_id": self.snowflake_conn_id,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Makes a series of connections to snowflake to get the status of the query
        by async get_query_status function
        """
        hook = SnowflakeSQLAPIHookAsync(self.snowflake_conn_id)
        try:
            for query_id in self.query_ids:
                while True:
                    run_state = await hook.get_query_status(query_id)
                    if run_state.status_code == 202:
                        await asyncio.sleep(self.poll_interval)
                    elif run_state.status_code == 422:
                        error_message = f"{self.task_id} failed with terminal state: {run_state}"
                        yield TriggerEvent({"status": "error", "message": error_message})
            yield TriggerEvent({"status": "success", "query_ids": self.query_ids})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
