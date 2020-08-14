import os
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator, SkipMixin
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.jid_plugins import JIDJobOperator

from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from airflow.exceptions import AirflowException, AirflowSkipException

from airflow.models import Variable

from airflow.utils.decorators import apply_defaults

import requests

import logging
LOG = logging.getLogger(__name__)
dag_name = os.path.splitext(os.path.basename(__file__))[0]


experiment = 'cxic0515'


default_args = {
        'start_date': datetime( 2020,1,1 ),
}

dag = DAG(
    dag_name,
    default_args=default_args,
    description='AutoSFX unit-cell consensus DAG',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=900,
  )

##### PROCESS DEFINITIONS
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

  def execute(self, context):
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
    context['ti'].xcom_push( key='return_value', value=tags )
    # TODO lets keep a cache of these to compare if they've changd
    
    
    raise AirflowSkipException("nothing")    
    

config = TagSensor( task_id='config',
    experiment=experiment,
    dag=dag,
  )

consensus = JIDJobOperator( task_id='consensus',
    experiment=experiment,
    run_id='',
    executable="/project/projectdirs/lcls/SFX_automation/consensus/submit.sh",
    parameters='',
    dag=dag,
  )

trigger = DummyOperator( task_id='trigger', 
  dag = dag,
  )

#### DRAW THE DAG

config >> consensus >> trigger


