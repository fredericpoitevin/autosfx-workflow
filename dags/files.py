import os
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator, SkipMixin
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.jid_plugins import LsSensor, GetFileSensor, PutFileOperator, JIDJobOperator
from airflow.operators.files_plugins import BulkFilesOperator

from airflow.exceptions import AirflowException, AirflowSkipException

import logging
LOG = logging.getLogger(__name__)

dag_name = os.path.splitext(os.path.basename(__file__))[0]

dag = DAG(
    dag_name,
    default_args={
        'start_date': datetime( 2020,1,1 ),
    },
    description='Example copy files',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=900,
  )

##### FILESENSORS


ls_dir = LsSensor( task_id='ls_dir',
    experiment='cxic0415',
    directory = '/global/project/projectdirs/lcls/temp/',
    dag=dag,
  )

get = GetFileSensor( task_id='get',
    experiment='cxic0415',
    filepath = '/global/project/projectdirs/lcls/temp/hello.world',
    dag=dag,
  )

put = PutFileOperator( task_id='put',
    experiment='cxic0415',
    filepath = '/global/project/projectdirs/lcls/temp/hello.world',
    data = 'it works...',
    dag = dag,
  )
  
get_again = GetFileSensor( task_id='get_again',
    experiment='cxic0415',
    filepath = '/global/project/projectdirs/lcls/temp/hello.world',
    dag=dag,
  )

copy_files = BulkFilesOperator( task_id='copy_files',
    experiment='cxic0415',
    dry_run=True,
    source='NERSC',
    dest='SLAC',
    paths=[ 'scratch', ],
    # paths=["scratch/*342*"],
    dag=dag,
  )


#### DRAW THE DAG

ls_dir >> get >> put >> get_again >> copy_files



