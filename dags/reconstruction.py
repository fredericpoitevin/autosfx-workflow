import os
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator, SkipMixin
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.jid_plugins import JIDOperator

from airflow.exceptions import AirflowException, AirflowSkipException

import logging
LOG = logging.getLogger(__name__)


class FileSensor( DummyOperator ):
  ui_color = '#b19cd9'

class ValueSensor( DummyOperator ):
  ui_color = '#CDEB8B'

dag_name = os.path.splitext(os.path.basename(__file__))[0]

default_args = {
        'start_date': datetime( 2020,1,1 ),
}

dag = DAG(
    dag_name,
    default_args=default_args,
    description='AutoSFX reconstruction DAG',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=900,
  )

##### FILESENSORS

#stream_file = FileSensor( task_id='stream_file',
#    bash_command="""exit 0""",
#    dag=dag,
#  )

##### PROCESS DEFINITIONS

merging = JIDOperator( task_id='merging',
    experiment='abcd',
    run=12,
    executable="/project/projectdirs/lcls/SFX_automation/merging/stream2mtz.slurm",
    parameters='',
    dag=dag,
  )

phasing = JIDOperator( task_id='phasing',
    experiment='abcd',
    run=12,
    executable="/project/projectdirs/lcls/SFX_automation/phasing/phasing.slurm",
    parameters='',
    dag=dag,
  )

#### DRAW THE DAG

merging >> phasing


