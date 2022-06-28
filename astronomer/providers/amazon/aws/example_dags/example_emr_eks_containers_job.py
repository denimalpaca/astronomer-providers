import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Optional

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from astronomer.providers.amazon.aws.operators.emr import EmrContainerOperatorAsync
from astronomer.providers.amazon.aws.sensors.emr import EmrContainerSensorAsync

# [START howto_operator_emr_eks_env_variables]
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "xxxxxxx")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "xxxxxxxxxxxx")
AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "xxxxxxxx")
CONTAINER_SUBMIT_JOB_POLICY = os.getenv(
    "CONTAINER_SUBMIT_JOB_POLICY", "test_emr_container_submit_jobs_policy"
)
DEBUGGING_MONITORING_POLICY = os.getenv("DEBUGGING_MONITORING_POLICY", "test_debugging_monitoring_policy")
EKS_CLUSTER_NAME = os.getenv("EKS_CLUSTER_NAME", "providers-team-eks-cluster")
EKS_NAMESPACE = os.getenv("EKS_NAMESPACE", "providers-team-eks-namespace")
INSTANCE_TYPE = os.getenv("INSTANCE_TYPE", "m4.large")
JOB_EXECUTION_POLICY = os.getenv("JOB_EXECUTION_POLICY", "test_job_execution_policy")
JOB_EXECUTION_ROLE = os.getenv("JOB_EXECUTION_ROLE", "test_iam_job_execution_role")
JOB_ROLE_ARN = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/{JOB_EXECUTION_ROLE}"
MANAGE_VIRTUAL_CLUSTERS = os.getenv("MANAGE_VIRTUAL_CLUSTERS", "test_manage_virtual_clusters")
VIRTUAL_CLUSTER_NAME = os.getenv("EMR_VIRTUAL_CLUSTER_NAME", "providers-team-virtual-eks-cluster")
# [END howto_operator_emr_eks_env_variables]

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def create_emr_virtual_cluster_func(task_instance: Any) -> None:
    """Create EMR virtual cluster in container"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("emr-containers")
    try:
        response = client.create_virtual_cluster(
            name=VIRTUAL_CLUSTER_NAME,
            containerProvider={
                "id": EKS_CLUSTER_NAME,
                "type": "EKS",
                "info": {"eksInfo": {"namespace": EKS_NAMESPACE}},
            },
        )
        task_instance.xcom_push(key="virtual_cluster_id", value=response["id"])
    except ClientError as error:
        logging.exception("Error while creating EMR virtual cluster")
        raise error


def delete_existing_cluster() -> None:
    """Get all virtual cluster in the namespace and if it is still running delete it"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("emr-containers")
    print("inside get cluster")
    try:
        response = client.list_virtual_clusters(
            containerProviderId=EKS_CLUSTER_NAME, containerProviderType="EKS"
        )
        print("response of get cluster ", response)
        if response and "virtualClusters" in response:
            for cluster in response["virtualClusters"]:
                print("cluster ", cluster)
                if cluster["name"] == VIRTUAL_CLUSTER_NAME and cluster["state"] == "RUNNING":
                    delete_emr_virtual_cluster_func(cluster["id"])
            delete_eks_cluster()
    except ClientError as error:
        logging.exception("Error while deleting EMR virtual cluster")
        raise error


def delete_eks_cluster() -> None:
    """Delete EKS cluster"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("eks")
    try:
        eks_status = get_eks_cluster_status(EKS_CLUSTER_NAME)
        print("eks_status ", eks_status)
        if eks_status and eks_status == "ACTIVE":
            client.delete_cluster(name=EKS_CLUSTER_NAME)
            while get_eks_cluster_status(EKS_CLUSTER_NAME) == "DELETING":
                logging.info("Waiting for cluster to be deleted. Sleeping for 30 seconds.")
                time.sleep(30)
    except ClientError as error:
        logging.info("Error while deleting EKS cluster")
        raise error


def delete_emr_virtual_cluster_func(virtual_cluster_id) -> None:
    """Delete EMR virtual cluster in container"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("emr-containers")
    try:
        client.delete_virtual_cluster(id=virtual_cluster_id)
        while get_cluster_status(virtual_cluster_id) == "TERMINATING":
            logging.info("Waiting for cluster to be deleted. Sleeping for 30 seconds.")
            time.sleep(30)
    except ClientError as error:
        logging.exception("Error while deleting EMR virtual cluster")
        raise error


def get_eks_cluster_status(cluster_name) -> Optional[str]:
    """Describe EKS virtual cluster status"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("eks")
    try:
        response = client.describe_cluster(name=cluster_name)
        if response and "cluster" in response:
            return response["cluster"]["status"]
        return None
    except ClientError:
        logging.info("No EKS cluster found")


def get_cluster_status(virtual_cluster_id) -> str:
    """Describe EMR virtual cluster status"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("emr-containers")
    try:
        response = client.describe_virtual_cluster(id=virtual_cluster_id)
        if response and "virtualCluster" in response:
            return response["virtualCluster"]["state"]
    except ClientError as error:
        logging.exception("Error while creating EMR virtual cluster")
        raise error


# [START howto_operator_emr_eks_config]
JOB_DRIVER_ARG = {
    "sparkSubmitJobDriver": {
        "entryPoint": "local:///usr/lib/spark/examples/src/main/python/pi.py",
        "sparkSubmitParameters": "--conf spark.executors.instances=2 --conf spark.executors.memory=2G --conf spark.executor.cores=2 --conf spark.driver.cores=1",  # noqa: E501
    }
}

CONFIGURATION_OVERRIDES_ARG = {
    "applicationConfiguration": [
        {
            "classification": "spark-defaults",
            "properties": {
                "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",  # noqa: E501
            },
        }
    ],
    "monitoringConfiguration": {
        "cloudWatchMonitoringConfiguration": {
            "logGroupName": "/aws/emr-eks-spark",
            "logStreamNamePrefix": "airflow",
        }
    },
}
# [END howto_operator_emr_eks_config]

with DAG(
    dag_id="example_emr_eks_pi_job",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "emr"],
) as dag:
    # Task steps for DAG to be self-sufficient
    setup_aws_config = BashOperator(
        task_id="setup_aws_config",
        bash_command=f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; ",
    )

    # Task to create EMR on EKS cluster
    create_cluster_environment_variables = (
        f"AWS_ACCOUNT_ID={AWS_ACCOUNT_ID} "
        f"AWS_DEFAULT_REGION={AWS_DEFAULT_REGION} "
        f"CONTAINER_SUBMIT_JOB_POLICY={CONTAINER_SUBMIT_JOB_POLICY} "
        f"DEBUGGING_MONITORING_POLICY={DEBUGGING_MONITORING_POLICY} "
        f"EKS_CLUSTER_NAME={EKS_CLUSTER_NAME} "
        f"EKS_NAMESPACE={EKS_NAMESPACE} "
        f"INSTANCE_TYPE={INSTANCE_TYPE} "
        f"JOB_EXECUTION_POLICY={JOB_EXECUTION_POLICY} "
        f"JOB_EXECUTION_ROLE={JOB_EXECUTION_ROLE} "
        f"MANAGE_VIRTUAL_CLUSTERS={MANAGE_VIRTUAL_CLUSTERS}"
    )

    # Task to delete EMR EKS virtual cluster as a pre step
    delete_emr_eks_virtual_cluster_pre_step = PythonOperator(
        task_id="delete_emr_eks_virtual_cluster_pre_step",
        python_callable=delete_existing_cluster,
    )

    create_eks_cluster_kube_namespace_with_role = BashOperator(
        task_id="create_eks_cluster_kube_namespace_with_role",
        bash_command=f"{create_cluster_environment_variables} "
        f"sh $AIRFLOW_HOME/dags/example_create_eks_kube_namespace_with_role.sh ",
    )

    # Task to create EMR virtual cluster
    create_emr_virtual_cluster = PythonOperator(
        task_id="create_emr_virtual_cluster",
        python_callable=create_emr_virtual_cluster_func,
    )

    VIRTUAL_CLUSTER_ID = (
        "{{ task_instance.xcom_pull(task_ids='create_emr_virtual_cluster', " "key='virtual_cluster_id') }}"
    )
    # [START howto_operator_run_emr_container_job]
    run_emr_container_job = EmrContainerOperatorAsync(
        task_id="run_emr_container_job",
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        execution_role_arn=JOB_ROLE_ARN,
        release_label="emr-6.2.0-latest",
        job_driver=JOB_DRIVER_ARG,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        name="pi.py",
    )
    # [END howto_operator_emr_eks_jobrun]

    # [START howto_sensor_emr_job_container_sensor]
    emr_job_container_sensor = EmrContainerSensorAsync(
        task_id="emr_job_container_sensor",
        job_id=run_emr_container_job.output,
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        poll_interval=5,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_sensor_emr_job_container_sensor]

    # Delete EKS cluster, EMR containers, IAM role and detach role policies.r
    removal_environment_variables = (
        f"AWS_ACCOUNT_ID={AWS_ACCOUNT_ID} "
        f"CONTAINER_SUBMIT_JOB_POLICY={CONTAINER_SUBMIT_JOB_POLICY} "
        f"DEBUGGING_MONITORING_POLICY={DEBUGGING_MONITORING_POLICY} "
        f"EKS_CLUSTER_NAME={EKS_CLUSTER_NAME} "
        f"JOB_EXECUTION_POLICY={JOB_EXECUTION_POLICY} "
        f"JOB_EXECUTION_ROLE={JOB_EXECUTION_ROLE} "
        f"MANAGE_VIRTUAL_CLUSTERS={MANAGE_VIRTUAL_CLUSTERS} "
        f"VIRTUAL_CLUSTER_ID={VIRTUAL_CLUSTER_ID}"
    )
    remove_cluster_container_role_policy = BashOperator(
        task_id="remove_cluster_container_role_policy",
        bash_command=f"{removal_environment_variables} "
        f"sh $AIRFLOW_HOME/dags/example_delete_eks_cluster_and_role_policies.sh ",
        trigger_rule="all_done",
    )

    # Task to Delete EMR EKS virtual cluster if in case it is still not deleted
    delete_emr_virtual_cluster = PythonOperator(
        task_id="delete_emr_virtual_cluster",
        python_callable=delete_emr_virtual_cluster_func,
        trigger_rule="all_done",
    )

    (
        setup_aws_config
        >> delete_emr_eks_virtual_cluster_pre_step
        >> create_eks_cluster_kube_namespace_with_role
        >> create_emr_virtual_cluster
        >> run_emr_container_job
        >> emr_job_container_sensor
        >> remove_cluster_container_role_policy
        >> delete_emr_virtual_cluster
    )
