"""This is an example dag for hive partition sensors."""
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.operators.bash_operator import BashOperator
from astronomer.providers.apache.hive.sensors.hive_partition import HivePartitionSensorAsync
from astronomer.providers.apache.hive.sensors.named_hive_partition import NamedHivePartitionSensorAsync

from airflow.utils.helpers import chain

HIVE_SCHEMA = os.getenv("HIVE_SCHEMA", "default")
HIVE_TABLE = os.getenv("HIVE_TABLE", "zipcode")
HIVE_PARTITION_1 = os.getenv("HIVE_PARTITION", "state='FL'")
HIVE_PARTITION_2 = os.getenv("HIVE_PARTITION", "state='AL'")
HIVE_PARTITION_3 = os.getenv("HIVE_PARTITION", "state='AZ'")
HIVE_PARTITION_4 = os.getenv("HIVE_PARTITION", "state='NC'")
HIVE_PARTITION_5 = os.getenv("HIVE_PARTITION", "state='PR'")
HIVE_PARTITION_6 = os.getenv("HIVE_PARTITION", "state='TX'")
JOB_FLOW_ROLE = os.getenv("EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole")
SERVICE_ROLE = os.getenv("EMR_SERVICE_ROLE", "EMR_DefaultRole")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}

with DAG(
    dag_id="example_hive_dag_1000_tasks",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "hive", "hive_partition"],
) as dag:
    tasks = [
        BashOperator(
            task_id="__".join(["tasks", "{}_of_{}".format(i, "200")]),
            bash_command="echo test",
        )
        for i in range(1, 200 + 1)
    ]
    chain(*tasks)
    # tasks = [
    #      NamedHivePartitionSensorAsync(
    #     task_id="__".join(["tasks", "{}_of_{}".format(i, "200")]),
    #     partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION_1}"],
    #     poke_interval=5,
    # )
    #     for i in range(1, 200 + 1)
    # ]
    # chain(*tasks)


    load_to_hive = HiveOperator(
        task_id="hive_query",
        hql=(
            "CREATE TABLE rajath_zipcode(RecordNumber int,Country string,City string,Zipcode int) "
            "PARTITIONED BY(state string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';"
            "CREATE TABLE rajath_zipcodes_tmp(RecordNumber int,Country string,City string,Zipcode int,state string)"
            "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';"
        ),
        trigger_rule="all_done",
    )
    hive_sensor_1 = HivePartitionSensorAsync(
        task_id="hive_partition_check_1",
        table=HIVE_TABLE,
        partition=HIVE_PARTITION_1,
        poke_interval=5,
    )

    wait_for_partition_1 = NamedHivePartitionSensorAsync(
        task_id="wait_for_partition_1",
        partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION_1}"],
        poke_interval=5,
    )
    hive_sensor_2 = HivePartitionSensorAsync(
        task_id="hive_partition_check_2",
        table=HIVE_TABLE,
        partition=HIVE_PARTITION_1,
        poke_interval=5,
    )

    wait_for_partition_2 = NamedHivePartitionSensorAsync(
        task_id="wait_for_partition_2",
        partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION_1}"],
        poke_interval=5,
    )
    hive_sensor_3 = HivePartitionSensorAsync(
        task_id="hive_partition_check_3",
        table=HIVE_TABLE,
        partition=HIVE_PARTITION_1,
        poke_interval=5,
    )

    wait_for_partition_3 = NamedHivePartitionSensorAsync(
        task_id="wait_for_partition_3",
        partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION_1}"],
        poke_interval=5,
    )
    hive_sensor_4 = HivePartitionSensorAsync(
        task_id="hive_partition_check_4",
        table=HIVE_TABLE,
        partition=HIVE_PARTITION_1,
        poke_interval=5,
    )

    wait_for_partition_4 = NamedHivePartitionSensorAsync(
        task_id="wait_for_partition_4",
        partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION_1}"],
        poke_interval=5,
    )
    hive_sensor_5 = HivePartitionSensorAsync(
        task_id="hive_partition_check_5",
        table=HIVE_TABLE,
        partition=HIVE_PARTITION_1,
        poke_interval=5,
    )

    wait_for_partition_5 = NamedHivePartitionSensorAsync(
        task_id="wait_for_partition_5",
        partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION_1}"],
        poke_interval=5,
    )
    # hive_sensor_6 = HivePartitionSensorAsync(
    #     task_id="hive_partition_check_6",
    #     table=HIVE_TABLE,
    #     partition=HIVE_PARTITION_1,
    #     poke_interval=5,
    # )

    # wait_for_partition_6 = NamedHivePartitionSensorAsync(
    #     task_id="wait_for_partition_6",
    #     partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION_1}"],
    #     poke_interval=5,
    # )
    # hive_sensor_7 = HivePartitionSensorAsync(
    #     task_id="hive_partition_check_7",
    #     table=HIVE_TABLE,
    #     partition=HIVE_PARTITION_1,
    #     poke_interval=5,
    # )

    # wait_for_partition_7 = NamedHivePartitionSensorAsync(
    #     task_id="wait_for_partition_7",
    #     partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION_1}"],
    #     poke_interval=5,
    # )
    # hive_sensor_8 = HivePartitionSensorAsync(
    #     task_id="hive_partition_check_8",
    #     table=HIVE_TABLE,
    #     partition=HIVE_PARTITION_1,
    #     poke_interval=5,
    # )

    # wait_for_partition_8 = NamedHivePartitionSensorAsync(
    #     task_id="wait_for_partition_8",
    #     partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION_1}"],
    #     poke_interval=5,
    # )
    # hive_sensor_9 = HivePartitionSensorAsync(
    #     task_id="hive_partition_check_9",
    #     table=HIVE_TABLE,
    #     partition=HIVE_PARTITION_1,
    #     poke_interval=5,
    # )

    # wait_for_partition_9 = NamedHivePartitionSensorAsync(
    #     task_id="wait_for_partition_9",
    #     partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION_1}"],
    #     poke_interval=5,
    # )
    # hive_sensor_10 = HivePartitionSensorAsync(
    #     task_id="hive_partition_check_10",
    #     table=HIVE_TABLE,
    #     partition=HIVE_PARTITION_1,
    #     poke_interval=5,
    # )

    # wait_for_partition_10 = NamedHivePartitionSensorAsync(
    #     task_id="wait_for_partition_10",
    #     partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION_1}"],
    #     poke_interval=5,
    # )
    [hive_sensor_1 >> wait_for_partition_1 >> hive_sensor_2 >> wait_for_partition_2 >> hive_sensor_3 >> hive_sensor_4 >> wait_for_partition_3 >> wait_for_partition_4 >> hive_sensor_5 >> wait_for_partition_5]
    # [hive_sensor_6 >> wait_for_partition_6 >> hive_sensor_7 >> wait_for_partition_7 >> hive_sensor_8 >> hive_sensor_9 >> wait_for_partition_8 >> wait_for_partition_9 >> hive_sensor_10 >> wait_for_partition_10]