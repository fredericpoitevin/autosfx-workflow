import os
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator, SkipMixin
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.bash_operator import BashOperator

from airflow.operators.jid_plugins import JIDOperator
from airflow.operators.jid_plugins import JIDJobOperator
from airflow.operators.jid_plugins import LsSensor, GetFileSensor, PutFileOperator, JIDOperator
from airflow.operators.files_plugins import BulkFilesOperator

from airflow.exceptions import AirflowException, AirflowSkipException

import logging
LOG = logging.getLogger(__name__)


#==========================================================\
# DAG SETUP (should not need lots of edits)

description='AutoSFX unit-cell determinationDAG'
dag_name = os.path.splitext(os.path.basename(__file__))[0]

default_args = {
        'start_date': datetime( 2020,1,1 ),
}

dag = DAG(
    dag_name,
    default_args=default_args,
    description=description,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=900,
  )
# DAG SETUP DONE
#==========================================================/


#==========================================================\
# TASKS SETUP

scriptdir = "/project/projectdirs/lcls/SFX_automation/"
peak_finding_script = scriptdir+"peak_finding/submit-testfred.sh"
indexing_script = scriptdir+"indexing/submit-testfred.sh"

#config = BashOperator( task_id='config',
#  bash_command="""echo {{ dag_run.conf }}""",
#  dag=dag
#)

peak_finding = JIDJobOperator( task_id='peak_finding',
    experiment="{{ dag_run.conf['experiment'] }}",
    run_id="{{ dag_run.conf['run_id'] }}",
    executable=peak_finding_script,
    parameters="{{ dag_run.conf['experiment'] }} {{ dag_run.conf['run_id'] }} {{ dag_run.conf['detector'] }} {{ dag_run.conf['JID_UPDATE_COUNTERS'] }}",
    dag=dag,
  )

status_peaks = GetFileSensor( task_id='status_peaks',
    experiment = "{{ dag_run.conf['experiment'] }}",
    filepath = "/global/cfs/cdirs/lcls/exp/{{ dag_run.conf['experiment'][:3] }}/{{ dag_run.conf['experiment'] }}/scratch/testfred/status_peaks.txt",
    dag=dag,
  )

indexing = JIDOperator( task_id='indexing',
    experiment='abcd',
    run=0,
    executable=indexing_script,
    parameters='',
    dag=dag,
  )

#### DRAW THE DAG

peak_finding >> indexing


