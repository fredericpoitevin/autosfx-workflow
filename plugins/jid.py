import os
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator, SkipMixin
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.plugins_manager import AirflowPlugin

from airflow.utils.decorators import apply_defaults

import time
import requests

import logging
LOG = logging.getLogger(__name__)

class JIDOperator( BaseOperator ):
  ui_color = '#006699'
  template_fields = ('executable',)

  locations = {
    'NERSC': "https://pslogin01:8443/jid_nersc", 
  }
  endpoints = {
    'start_job': '/jid/ws/start_job',
    'job_statuses': '/jid/ws/job_statuses',
    'job_log_file': '/jid/ws/job_log_file',
  }

  @apply_defaults
  def __init__(self, 
      experiment=None, run_id=25, 
      executable=None, parameters=None, 
      user='mshankar', 
      run_at='NERSC',
      poke_interval=30,
      cert='/usr/local/airflow/dags/certs/airflow.crt', key='/usr/local/airflow/dags/certs/airflow.key', root_ca='/usr/local/airflow/dags/certs/rootCA.crt', xcom_push=True, 
      *args, **kwargs ):
    super(JIDOperator, self).__init__(*args, **kwargs)
    self.workflow_id = experiment
    self.experiment = experiment
    self.run_id = run_id
    self.user = user
    self.executable = executable
    self.parameters = parameters
    self.run_at = run_at
    self.poke_interval = poke_interval
    self.requests_opts = {
      "cert": ( cert, key ),
      "verify": False,
    }


  def create_control_doc( self, context ):
    def_id = 'anything'
    return { 
      "_id" : self.workflow_id,
      "experiment": self.experiment,
      "run_num" : self.run_id,
      "user" : self.user,
      "status" : '',
      "tool_id" : '',
      "def_id" : def_id,
      "def": {
        "_id" : def_id,
        "name" : context['task'].task_id,
        "executable" : str(self.executable),
        "trigger" : "MANUAL",
        "location" : self.run_at,
        "parameters" : self.parameters,
        "run_as_user" : self.user
      }
    }
    
  def rpc( self, endpoint, control_doc ):

    if not self.run_at in self.locations:
      raise AirflowException(f"JID location {self.run_at} is not configured")
    LOG.info(f"Attempting to run at {self.run_at}...")

    uri = self.locations[self.run_at] + self.endpoints[endpoint]
    LOG.info( f"Calling {uri} with {control_doc}..." )
    resp = requests.post( uri, json=control_doc, **self.requests_opts )
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

  def execute(self, context):

    control_doc = self.create_control_doc( context )

    msg = self.rpc( 'start_job', control_doc )
    LOG.info(f"GOT job {msg}")
    jobs = [ msg, ]
    # pool and wait
    # TODO: ensure we don't do this forever
    while jobs[0].get('status') in ('RUNNING', 'SUBMITTED'):
      jobs = self.rpc( 'job_statuses', jobs )
      LOG.info("  waiting...")
      time.sleep(self.poke_interval)

    # grab logs and put into xcom
    out = self.rpc( 'job_log_file', jobs[0] )
    context['task_instance'].xcom_push(key='log',value=out)


class JIDPlugins(AirflowPlugin):
    name = 'jid_plugins'
    operators = [JIDOperator,]
