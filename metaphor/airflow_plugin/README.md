# Airflow Connector

This connector provides an Airflow lineage backend to extract lineage information.

## Setup

1. Install `metaphor-connectors` to your Airflow instance.

``` bash
pip install metaphor-connectors
```

2. Add the following config to `airflow.cfg` to send the linage information via API:

``` cfg
[lineage]
backend = metaphor.airflow_plugin.lineage.backend.MetaphorBackend
metaphor_backend_mode = ingestion-api
metaphor_ingestion_url = [URL]
metaphor_ingestion_key = [KEY]
```

Or, you can send lineage information to a s3 bucket.
``` cfg
[lineage]
backend = metaphor.airflow_plugin.lineage.backend.MetaphorBackend
metaphor_backend_mode = s3
metaphor_s3_url = s3://[bucket]
metaphor_aws_access_key_id = [AWS_ACCESS_KEY]
metaphor_aws_secret_access_key = [AWS_SECRET_KEY]
metaphor_assume_role_arn = [AWS_ROLE_ARN]
```

3. Configure inlets and outlets for your Airflow operators. For example,

``` python
from airflow.operators.bash import BashOperator
from airflow.lineage import AUTO
from airflow.models import DAG
from airflow.utils.dates import days_ago
from metaphor.airflow_plugin.lineage.entity import MetaphorDataset, DataPlatform

args = {"owner": "airflow", "start_date": days_ago(2)}

dag = DAG(
    dag_id="lineage_1",
    default_args=args,
    schedule_interval=None,
)

run_this_last = BashOperator(
    task_id="run_this_last",
    bash_command="echo 2",
    dag=dag,
    inlets=AUTO,
    outlets=[
        MetaphorDataset(
            database="metaphor",
            schema="public",
            table="bar",
            platform=DataPlatform.REDSHIFT,
        )
    ],
)

run_this = BashOperator(
    task_id="run_me_first",
    bash_command="echo 1",
    dag=dag,
    inlets=[
        MetaphorDataset(
            database="metaphor",
            schema="public",
            table="raw_1",
            platform=DataPlatform.REDSHIFT,
        ),
        MetaphorDataset(
            database="metaphor",
            schema="public",
            table="raw_2",
            platform=DataPlatform.REDSHIFT,
        ),
    ],
    outlets=[
        MetaphorDataset(
            database="metaphor",
            schema="public",
            table="foo",
            platform=DataPlatform.REDSHIFT
        )
    ],
)

run_this.set_downstream(run_this_last)
```

See [Airflow Lineage](https://airflow.apache.org/docs/apache-airflow/stable/lineage.html) doc for more details.
