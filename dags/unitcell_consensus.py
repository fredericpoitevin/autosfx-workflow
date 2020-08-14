import os
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator, SkipMixin
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.jid_plugins import JIDJobOperator

from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from airflow.exceptions import AirflowException, AirflowSkipException

from airflow.utils.decorators import apply_defaults

import requests

import logging
LOG = logging.getLogger(__name__)
dag_name = os.path.splitext(os.path.basename(__file__))[0]

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
      url='https://pswww.slac.stanford.edu/ws-auth/lgbk/lgbk/', 
      path='/ws/map_param_editable_to_run_nums', 
      param='TAG', *args, **kwargs ):
    super( TagSensor, self ).__init__(*args, **kwargs)
    self.experiment = experiment
    self.url = url
    self.path = path
    self.param = param

  def execute(self, context):
    raise AirflowSkipException("nothing")    
    

config = TagSensor( task_id='config',
    experiment='cxic0415',
    dag=dag,
  )

consensus = JIDJobOperator( task_id='consensus',
    experiment='cxic0415',
    run_id=0,
    executable="/project/projectdirs/lcls/SFX_automation/consensus/submit.sh",
    parameters='',
    dag=dag,
  )

#### DRAW THE DAG

config >> consensus


