import os
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator, SkipMixin
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.bash_operator import BashOperator

from airflow.operators.jid_plugins import LsSensor, GetFileSensor, PutFileOperator, JIDJobOperator, JIDSlurmOperator
from airflow.operators.files_plugins import BulkFilesOperator

from airflow.exceptions import AirflowException, AirflowSkipException

from airflow.utils.decorators import apply_defaults


from airflow.models import Variable
import requests

import logging
LOG = logging.getLogger(__name__)


#==========================================================\
# DAG SETUP (should not need lots of edits)

description='AutoSFX unit-cell determinationDAG'
dag_name = os.path.splitext(os.path.basename(__file__))[0]

class TagSensor( BaseOperator ):

  template_fields = ('experiment_name',)

  @apply_defaults
  def __init__(self,
      experiment_name: str,
      url='https://pswww.slac.stanford.edu/ws-auth/lgbk/lgbk',
      path='/ws/map_param_editable_to_run_nums',
      param='TAG', *args, **kwargs ):
    super( TagSensor, self ).__init__(*args, **kwargs)
    self.experiment_name = experiment_name
    self.url = url
    self.path = path
    self.param = param

  def fetch(self, context):
    # logbook endpoint to get info
    endpoint = f"{self.url}/{self.experiment_name}/{self.path}?param_name={self.param}"
    # uth
    session = requests.Session()
    instrument = self.experiment_name[:3]
    session.auth = ( f'{instrument}opr', Variable.get( f'lcls-logbook_{instrument}opr' ) )
    LOG.info( f"GET {endpoint}" )
    resp = session.get( endpoint )
    LOG.info(f" + {resp.status_code}: {resp.content.decode('utf-8')}")
    if not resp.status_code in ( 200, ):
      raise AirflowException(f"Bad response for query {resp}: {resp.content}")
    j = resp.json()
    if not j.get( 'success' ) == True:
      raise AirflowException(f"Failed response: {resp.content}")

    # get tags
    tags = j.get('value')
    for tag,runs in tags.items():
      LOG.info(f"TAG {tag} consists of runs {runs}" )
    # TODO lets keep a cache of these to compare if they've changd
    return tags

  def execute( self, context ):
    tags = self.fetch( context )
    context['ti'].xcom_push( key='return_value', value=tags )
    raise NotImplementedError("nothing to do")


class IsRunTaggedSensor( TagSensor ):

  template_fields = ( 'experiment_name', 'run_num' )

  @apply_defaults
  def __init__( self,
    run_num: str, *args, **kwargs ):
    super( IsRunTaggedSensor, self ).__init__(*args,**kwargs)
    self.run_num = run_num

  def execute( self, context ):
    tags = self.fetch( context )
    for tag, runs in tags.items():
      if int(self.run_num) in runs:
        LOG.info( f"Run {self.run_num} has been tagged {tag}, continuing..." )
        #context['ti'].xcom_push( key='return_value', value=tag )
        return tag

    raise AirflowSkipException( f"Run {self.run_num} has not been tagged in the logbook" ) 

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


tag = IsRunTaggedSensor( task_id='tag',
    experiment_name="{{ dag_run.conf['experiment_name'] }}",
    run_num="{{ dag_run.conf['run_num'] }}",
    param='TAG',
    dag=dag,
  )


#peak_finding = JIDJobOperator( task_id='peak_finding',
#    experiment="{{ dag_run.conf['experiment'] }}",
#    run_id="{{ dag_run.conf['run_id'] }}",
#    executable=peak_finding_script,
#    parameters="{{ dag_run.conf['experiment'] }} {{ dag_run.conf['run_id'] }} {{ dag_run.conf['detector'] }} {{ dag_run.conf['JID_UPDATE_COUNTERS'] }} {{ task_instance.xcom_pull(task_ids='tag') }}",
#    dag=dag,
#  )

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
#SBATCH --time=01:00:00
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
srun -n 64 shifter sh {{ script }}
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

#prevent crash when running on one core
export HDF5_USE_FILE_LOCKING=FALSE

# define output directory
outdir="${SIT_PSDM_DATA}/{{ inst }}/{{ exp }}/scratch/r{{ run }}/peak-finding/"
[[ ! -d $outdir ]] && mkdir -p -m777 $outdir

# run!
echo findPeaks -e {{ exp }} -r {{ run }} \
    --instrument {{ inst }} -d {{ detector }} \
    --tag {{ ti.xcom_pull( task_ids='tag' ) }} \
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
    --mask $outdir/staticMask.h5 \
    --clen CXI:DS2:MMS:06.RBV \
    --coffset 0.3189938 \
    --minPeaks 15 \
    --maxPeaks 2048 \
    --minRes -1 \
    --sample sample \
    --pixelSize 0.00010992 \
    --auto False \
    --detectorDistance 0.144 \
    --access ana
""",
    dag=dag,
  )


#status_peaks = GetFileSensor( task_id='status_peaks',
#    experiment = "{{ dag_run.conf['experiment'] }}",
#    filepath = "/global/cfs/cdirs/lcls/exp/{{ dag_run.conf['experiment'][:3] }}/{{ dag_run.conf['experiment'] }}/scratch/testfred/status_peaks.txt",
#    dag=dag,
#  )

indexing = JIDJobOperator( task_id='indexing',
    experiment='abcd',
    run_id=0,
    executable=indexing_script,
    parameters='',
    dag=dag,
  )

#### DRAW THE DAG

tag >> peak_finding >> indexing


