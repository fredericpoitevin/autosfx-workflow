import os
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator, SkipMixin
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.jid_plugins import JIDOperator

from airflow.exceptions import AirflowException, AirflowSkipException

import logging
LOG = logging.getLogger(__name__)


#==========================================================\
# DAG SETUP (should not need lots of edits)

description='AutoSFX geometry calibration DAG'
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

scriptdir="/project/projectdirs/lcls/SFX_automation/geomcalib"
testmpi_script    = scriptdir+"/test-mpi.slurm"
setup_script      = scriptdir+"/setup.slurm"
powdersum_script  = scriptdir+"/powdersum.slurm"
findcenter_script = scriptdir+"/findcenter.slurm"
peakfinder_script = scriptdir+"/peakfinder.slurm"

#testmpi = JIDOperator( task_id='testmpi',
#    experiment='abcd',
#    run=12,
#    executable=testmpi_script,
#    parameters='',
#    dag=dag,
#  )

#setup = JIDOperator( task_id='setup',
#    experiment='abcd',
#    run=12,
#    executable=setup_script,
#    parameters='',
#    dag=dag,
#  )

#powdersum = JIDOperator( task_id='powdersum',
#    experiment='abcd',
#    run=12,
#    executable=powdersum_script,
#    parameters='',
#    dag=dag,
#  )

#findcenter = JIDOperator( task_id='findcenter',
#    experiment='abcd',
#    run=12,
#    executable=findcenter_script,
#    parameters='',
#    dag=dag,
#  )

peakfinder = JIDOperator( task_id='peakfinder',
    experiment='abcd',
    run=12,
    executable=peakfinder_script,
    parameters='',
    dag=dag,
  )

# TASKS SETUP DONE
#==========================================================/


#==========================================================\
# DAG DESIGN

#testmpi
#setup >> powdersum
#findcenter
peakfinder

# DAG DESIGN DONE
#==========================================================/
