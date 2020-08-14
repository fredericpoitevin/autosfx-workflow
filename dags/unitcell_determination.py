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

import logging
LOG = logging.getLogger(__name__)


#==========================================================\
# DAG SETUP (should not need lots of edits)

description='AutoSFX unit-cell determinationDAG'
dag_name = os.path.splitext(os.path.basename(__file__))[0]

class TagSensor( BaseOperator ):

  template_fields = ('experiment',)

  @apply_defaults
  def __init__(self,
      experiment: str,
      url='https://pswww.slac.stanford.edu/ws-auth/lgbk/lgbk',
      path='/ws/map_param_editable_to_run_nums',
      param='TAG', *args, **kwargs ):
    super( TagSensor, self ).__init__(*args, **kwargs)
    self.experiment = experiment
    self.url = url
    self.path = path
    self.param = param

  def fetch(self, context):
    # logbook endpoint to get info
    endpoint = f"{self.url}/{self.experiment}/{self.path}?param_name={self.param}"
    # uth
    session = requests.Session()
    instrument = self.experiment[:3]
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

  template_fields = ( 'experiment', 'run_id' )

  @apply_defaults
  def __init__( self,
    run_id: str, *args, **kwargs ):
    super( IsRunTaggedSensor, self ).__init__(*args,**kwargs)
    self.run_id = run_id

  def execute( self, context ):
    tags = self.fetch( context )
    for tag, runs in tags.items():
      if int(self.run_id) in runs:
        LOG.info( f"Run {self.run_id} has been tagged {tag}, continuing..." )
        #context['ti'].xcom_push( key='return_value', value=tag )
        return tag

    raise AirflowSkipException( f"Run {self.run_id} has not been tagged in the logbook" ) 

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
    experiment="{{ dag_run.conf['experiment'] }}",
    run_id="{{ dag_run.conf['run_id'] }}",
    param='cheese',
    dag=dag,
  )


peak_finding = JIDJobOperator( task_id='peak_finding',
    experiment="{{ dag_run.conf['experiment'] }}",
    run_id="{{ dag_run.conf['run_id'] }}",
    executable=peak_finding_script,
    parameters="{{ dag_run.conf['experiment'] }} {{ dag_run.conf['run_id'] }} {{ dag_run.conf['detector'] }} {{ dag_run.conf['JID_UPDATE_COUNTERS'] }} {{ task_instance.xcom_pull(task_ids='tag' }}",
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


