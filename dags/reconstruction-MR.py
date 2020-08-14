import os
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator, SkipMixin
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.jid_plugins import LsSensor, GetFileSensor, PutFileOperator, JIDOperator
from airflow.operators.files_plugins import BulkFilesOperator

from airflow.exceptions import AirflowException, AirflowSkipException

import logging
LOG = logging.getLogger(__name__)


#==========================================================\
# DAG SETUP (should not need lots of edits)

description='AutoSFX MR reconstruction DAG'
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

instrument    = 'cxi'
experiment    = 'cxic0515'

nersc_rootdir = '/global/cscratch1/sd/psdatmgr/data/psdm/'
nersc_srcdir  = '/global/project/projectdirs/lcls/SFX_automation/'
nersc_expdir  = nersc_rootdir+instrument+'/'+experiment+'/'
nersc_scratch = nersc_expdir+'scratch/fred'
#nersc_workdir = nersc_scratch+'/'+dag_name

config_file      = nersc_srcdir+'current_analysis.config'
testarg_script   = nersc_srcdir+'utils/testarg.slurm'
merging_script   = nersc_srcdir+'merging/stream2mtz.slurm'
MRphasing_script = nersc_srcdir+'phasing/MRphasing.slurm'
make_summary_script = nersc_srcdir+'phasing/summary.slurm'

ls_scratch = LsSensor( task_id='ls_scratch',
    directory = nersc_scratch+'/',
    dag=dag,
  )

update_config = PutFileOperator( task_id='update_config',
    filepath = config_file,
    data = experiment,
    dag = dag,
  )

send_user_data = BulkFilesOperator( task_id='send_user_data',
    experiment=experiment,
    dry_run=False,
    source='SLAC',
    dest='NERSC',
    paths=[ 'scratch/fred/user_data', ],
    dag=dag,
  )

merging = JIDOperator( task_id='merging',
    experiment='abcd',
    run=12,
    executable=merging_script,
    parameters='',
    dag=dag,
  )

MRphasing = JIDOperator( task_id='MRphasing',
    experiment='abcd',
    run=12,
    executable=MRphasing_script,
    parameters='',
    dag=dag,
  )

make_summary = JIDOperator( task_id='make_summary',
    experiment='abcd',
    run=12,
    executable=make_summary_script,
    parameters='',
    dag=dag,
  )

# TASKS SETUP DONE
#==========================================================/


#==========================================================\
# DAG DESIGN

update_config >> merging >> ls_scratch

send_user_data >> ls_scratch >> MRphasing >> make_summary


#ls_dir >> merging >> testarg
#copy_files
#testarg
#ls_dir >> testarg
#MRphasing

# DAG DESIGN DONE
#==========================================================/
