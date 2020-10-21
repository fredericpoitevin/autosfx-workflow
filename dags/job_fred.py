import os
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator, SkipMixin
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.bash_operator import BashOperator

from airflow.operators.jid_plugins import LsSensor, GetFileSensor, PutFileOperator, JIDJobOperator
from airflow.operators.files_plugins import BulkFilesOperator

from airflow.exceptions import AirflowException, AirflowSkipException

from airflow.utils.decorators import apply_defaults


from airflow.models import Variable
import requests
import time
import logging
import os

LOG = logging.getLogger(__name__)


#==========================================================\
# DAG SETUP (should not need lots of edits)

description='Submit slurm job'
dag_name = os.path.splitext(os.path.basename(__file__))[0]



class JIDSlurmOperator( BaseOperator ):

  ui_color = '#006699'

  locations = {
    'NERSC': "https://pslogin01:8443/jid_nersc/jid/ws",
  }
  endpoints = {
    'start_job': '/start_job',
    'job_statuses': '/job_statuses',
    'job_log_file': '/job_log_file',
    'file': '/file',
  }

  template_fields = ['slurm_script','bash_commands',]

  @apply_defaults
  def __init__(self,
      slurm_script: str,
      bash_commands: str,
      user='mshankar',
      run_at='NERSC',
      poke_interval=30,
      working_dir='/global/project/projectdirs/lcls/SFX_automation/jobs', 
      cert='/usr/local/airflow/dags/certs/airflow.crt', 
      key='/usr/local/airflow/dags/certs/airflow.key', 
      root_ca='/usr/local/airflow/dags/certs/rootCA.crt', 
      xcom_push=True,
      *args, **kwargs ):

    super(JIDSlurmOperator, self).__init__(*args, **kwargs)

    self.slurm_script = slurm_script
    self.bash_commands = bash_commands

    self.working_dir = working_dir
    self.user = user
    self.run_at = run_at
    self.poke_interval = poke_interval
    self.requests_opts = {
      "cert": ( cert, key ),
      "verify": False,
    }

  def create_control_doc( self, context, executable, parameters ):
    return {
      "_id" : executable,
      "experiment": context.get('dag_run').conf.get('experiment_name'),
      "run_num" : context.get('dag_run').conf.get('run_num'),
      "user" : self.user,
      "status" : '',
      "tool_id" : '', # lsurm job id
      "def_id" : executable,
      "def": {
        "_id" : executable,
        "name" : context['task'].task_id,
        "executable" : f"{self.working_dir}/{executable}.slurm",
        "trigger" : "MANUAL",
        "location" : self.run_at,
        "parameters" : parameters,
        "run_as_user" : self.user
      }
    }

  def get_file_uri( self, filepath ):
    if not self.run_at in self.locations:
      raise AirflowException(f"JID location {self.run_at} is not configured")
    uri = self.locations[self.run_at] + self.endpoints['file'] + filepath
    return uri

  def parse( self, resp ):
    LOG.info(f"  {resp.status_code}: {resp.content}")
    if not resp.status_code in ( 200, ):
      raise AirflowException(f"Bad response from JID {resp}: {resp.content}")
    try:
      j = resp.json()
      if not j.get('success',"") in ( True, ):
        raise AirflowException(f"Error from JID {resp}: {resp.content}")
      return j.get('value')
    except Exception as e:
      raise AirflowException(f"Response from JID not parseable: {e}")

  def put_file( self, path, content ):
    uri = self.get_file_uri( path )
    LOG.info( f"Calling {uri}..." )
    resp = requests.put( uri, data=content, **self.requests_opts )
    v = self.parse( resp )
    if 'status' in v and v['status'] == 'ok':
      return True
    return False

  def rpc( self, endpoint, control_doc, check_for_error=[] ):

    if not self.run_at in self.locations:
      raise AirflowException(f"JID location {self.run_at} is not configured")

    uri = self.locations[self.run_at] + self.endpoints[endpoint]
    LOG.info( f"Calling {uri} with {control_doc}..." )
    resp = requests.post( uri, json=control_doc, **self.requests_opts )
    LOG.info(f" + {resp.status_code}: {resp.content.decode('utf-8')}")
    if not resp.status_code in ( 200, ):
      raise AirflowException(f"Bad response from JID {resp}: {resp.content}")
    try:
      j = resp.json()
      if not j.get('success',"") in ( True, ):
        raise AirflowException(f"Error from JID {resp}: {resp.content}")
      v = j.get('value')
      # throw error if the response has any matching text: TODO use regex
      for i in check_for_error:
        if i in v:
          raise AirflowException(f"Response failed due to string match {i} against response {v}")
      return v
    except Exception as e:
      raise AirflowException(f"Response from JID not parseable: {e}")

  def execute( self, context ):

    LOG.info(f"Attempting to run at {self.run_at}...")

    this = f"{context.get('dag_run').conf.get('experiment_name')}-{context.get('dag_run').conf.get('run_num')}-{context.get('task').task_id}"

    # upload slurm script it to the destination
    LOG.info("Uploading job scripts...")
    job_file = f"{self.working_dir}/{this}.slurm"
    self.put_file( job_file, self.slurm_script )

    command_file = f"{self.working_dir}/{this}.sh"
    self.put_file( command_file, self.bash_commands )

    # run job for it
    LOG.info("Queueing slurm job...")
    control_doc = self.create_control_doc( context, this, '' )
    msg = self.rpc( 'start_job', control_doc )
    LOG.info(f"jobid {msg['tool_id']} successfully submitted!")
    jobs = [ msg, ]

    # FIXME: initial wait for job to queue
    time.sleep(10)
    LOG.info("Checking for job completion...")
    while jobs[0].get('status') in ('RUNNING', 'SUBMITTED'):
      jobs = self.rpc( 'job_statuses', jobs, check_for_error=( ' error: ', 'Traceback ' ) )
      time.sleep(self.poke_interval)

    # grab logs and put into xcom
    out = self.rpc( 'job_log_file', jobs[0] )
    context['task_instance'].xcom_push(key='log',value=out) 






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



peak_finding = JIDSlurmOperator( task_id='peak_finding',
    slurm_script="""#!/bin/bash -l
{%- set exp = dag_run.conf.get('experiment_name') %}
{%- set run = dag_run.conf.get('run_num') %}
{%- set jid_update_url = dag_run.conf.get('JID_UPDATE_COUNTERS') %}
{%- set script = exp + '-' + run + '-' ~ task.task_id + '.sh' %}

#SBATCH --account=lcls
#SBATCH --job-name={{ script }}
#SBATCH --nodes=2
#SBATCH --constraint=haswell
#SBATCH --time=00:05:00
#SBATCH --image=docker:slaclcls/crystfel:latest
###SBATCH --exclusive
#SBATCH --qos=realtime

t_start=`date +%s`
export PMI_MMAP_SYNC_WAIT_TIME=600

# send updates back to logbook
echo ./report.sh {{ jid_update_url }}
#./report.sh {{ jid_update_url }} &

# run the peakfinding
echo "srun -n 64 shifter sh {{ script }}"
# srun -n 64 shifter sh {{ script }}
RC=$?

cat {{ script }}

t_end=`date +%s`
echo PSJobCompleted TotalElapsed $((t_end-t_start)) $t_start $t_end

exit $RC
""",
    bash_commands="""#!/bin/bash
{%- set exp = dag_run.conf.get('experiment_name') %}
{%- set run = dag_run.conf.get('run_num') %}
{%- set detector = dag_run.conf.get('detector') %}
{%- set inst = exp[:3] %}

#prevent crash when running on one core
export HDF5_USE_FILE_LOCKING=FALSE

# activate psana environment
source /img/conda.local/env.local
source activate psana_base

# set location for experiment db and calib dir
export SIT_DATA=$CONDA_PREFIX/data
export SIT_PSDM_DATA=/global/cscratch1/sd/psdatmgr/data/psdm

#PSOCAKE
export PATH=/project/projectdirs/lcls/SFX_automation/psocake/app:$PATH
export PYTHONPATH=/project/projectdirs/lcls/SFX_automation/psocake:$PYTHONPATH
export PSOCAKE_FACILITY=LCLS
peakfinder="findPeaksCori1"

# define output directory
outdir="${SIT_PSDM_DATA}/{{ inst }}/{{ exp }}/scratch/r{{ run }}/peak-finding/"
[[ ! -d $outdir ]] && mkdir -p -m777 $outdir

# parameters
mask="/project/projectdirs/lcls/SFX_automation/peak_finding/staticMask.h5"
clen="CXI:DS2:MMS:06.RBV"

# run!
echo {{ peakfinder }} \
    --exp {{ exp }} \
    --run {{ run }} \
    --instrument {{ inst }} \
    --det {{ detector }} \
    --tag TAG \
    --outDir $outdir \
    --algorithm 2 \
    --alg_npix_min 2.0 \
    --alg_npix_max 30.0 \
    --alg_amax_thr 300.0 \
    --alg_atot_thr 600.0 \
    --alg_son_min 10.0 \
    --alg1_thr_low 0.0 \
    --alg1_thr_high 0.0 \
    --alg1_rank 3 \
    --alg1_radius 3 \
    --alg1_dr 2 \
    --psanaMask_on True \
    --psanaMask_calib True \
    --psanaMask_status True \
    --psanaMask_edges True \
    --psanaMask_central True \
    --psanaMask_unbond True \
    --psanaMask_unbondnrs True \
    --mask $mask \
    --sample sample \
    --clen $clen \
    --coffset 0.3189938 \
    --detectorDistance 0.144 \
    --pixelSize 0.00010992 \
    --minPeaks 15 \
    --maxPeaks 2048 \
    --minRes -1 \
    --auto False \
    --access ana
""",
    dag=dag,
  )

peak_finding


