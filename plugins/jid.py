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


class JIDBaseOperator( BaseOperator ):
  ui_color = '#006699'

  locations = {
    'NERSC': "https://pslogin01:8443/jid_nersc/jid/ws",
  }
  template_fields = ('experiment',)

  @apply_defaults
  def __init__(self,
      experiment: str,
      user='mshankar',
      run_at='NERSC',
      poke_interval=30,
      cert='/usr/local/airflow/dags/certs/airflow.crt', key='/usr/local/airflow/dags/certs/airflow.key', root_ca='/usr/local/airflow/dags/certs/rootCA.crt', xcom_push=True,
      *args, **kwargs ):
    super( JIDBaseOperator, self ).__init__(*args, **kwargs)
    self.experiment = experiment
    self.user = user
    self.run_at = run_at
    self.poke_interval = poke_interval
    self.requests_opts = {
      "cert": ( cert, key ),
      "verify": False,
    }


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
    
  def rpc( self, endpoint, control_doc, check_for_error=[] ):

    if not self.run_at in self.locations:
      raise AirflowException(f"JID location {self.run_at} is not configured")

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
      v = j.get('value')
      # throw error if the response has any matching text: TODO use regex
      for i in check_for_error:
        if i in v:
          raise AirflowException(f"Response failed due to string match {i} against response {v}")
      return v
    except Exception as e:
      raise AirflowException(f"Response from JID not parseable: {e}")

  def execute(self, context):

    LOG.info(f"Attempting to run at {self.run_at}...")
    control_doc = self.create_control_doc( context )

    msg = self.rpc( 'start_job', control_doc )
    LOG.info(f"GOT job {msg}")
    jobs = [ msg, ]
    # pool and wait
    # TODO: ensure we don't do this forever
    while jobs[0].get('status') in ('RUNNING', 'SUBMITTED'):
      jobs = self.rpc( 'job_statuses', jobs, check_for_error=( ' error: ', 'Traceback ' ) )
      LOG.info("  waiting...")
      time.sleep(self.poke_interval)

    # grab logs and put into xcom
    out = self.rpc( 'job_log_file', jobs[0] )
    context['task_instance'].xcom_push(key='log',value=out)



class JIDJobOperator( JIDBaseOperator ):

  ui_color = '#006699'
  template_fields = ['experiment', 'run_id', 'executable', 'parameters' ]

  endpoints = {
    'start_job': '/start_job',
    'job_statuses': '/job_statuses',
    'job_log_file': '/job_log_file',
  }

  @apply_defaults
  def __init__(self,
      run_id: str,
      executable: str, parameters: str,
      *args, **kwargs ):
    super(JIDJobOperator, self).__init__(*args, **kwargs)
    self.workflow_id = self.experiment
    self.run_id = run_id
    self.executable = executable
    self.parameters = parameters

  def create_control_doc( self, context ):
    def_id = f"{self.experiment}-{self.run_id}-{context['task'].task_id}"
    return {
      "_id" : def_id,
      "experiment": self.experiment,
      "run_num" : self.run_id,
      "user" : self.user,
      "status" : '',
      "tool_id" : '', # lsurm job id
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

  def execute(self, context):

    LOG.info(f"Attempting to run at {self.run_at}...")
    control_doc = self.create_control_doc( context )

    msg = self.rpc( 'start_job', control_doc )
    LOG.info(f"GOT job {msg}")
    jobs = [ msg, ]
    # pool and wait
    # TODO: ensure we don't do this forever
    time.sleep(10)
    while jobs[0].get('status') in ('RUNNING', 'SUBMITTED'):
      jobs = self.rpc( 'job_statuses', jobs, check_for_error=( ' error: ', 'Traceback ' ) )
      LOG.info("  waiting...")
      time.sleep(self.poke_interval)

    # grab logs and put into xcom
    out = self.rpc( 'job_log_file', jobs[0] )
    context['task_instance'].xcom_push(key='log',value=out)








class JIDFileBase( JIDBaseOperator ):
  ui_color = '#b19cd9'
  endpoints = {
    'file': '/file',
  }

  @apply_defaults
  def __init__(self,
    filepath=None,
    *args, **kwargs):
    super( JIDFileBase,  self ).__init__(*args, **kwargs)
    self.filepath = filepath

  def get_uri( self ):
    if not self.run_at in self.locations:
      raise AirflowException(f"JID location {self.run_at} is not configured")
    uri = self.locations[self.run_at] + self.endpoints['file'] + self.filepath
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

class LsSensor( JIDFileBase ):

  @apply_defaults
  def __init__(self,
    directory,
    *args, **kwargs):
    super( LsSensor,  self ).__init__(*args, **kwargs)
    self.directory = directory
    # full paths?

  def get_uri( self ):
    if not self.run_at in self.locations:
      raise AirflowException(f"JID location {self.run_at} is not configured")
    uri = self.locations[self.run_at] + self.endpoints['file'] + self.directory
    return uri

  def execute( self, context ):
    uri = self.get_uri()
    LOG.info( f"Calling {uri}..." )
    resp = requests.get( uri, **self.requests_opts )
    v = self.parse( resp )
    if not 'entries' in v:
      raise AirflowException(f"Error, could not find 'entries' in response")
    context['ti'].xcom_push( key='return_value', value=v['entries'] )


class GetFileSensor( JIDFileBase ):

  def execute( self, context ):
    uri = self.get_uri()
    LOG.info( f"Calling {uri}..." )
    resp = requests.get( uri, **self.requests_opts )
    #LOG.info(f"  {resp.status_code}: {resp.content}")
    if not resp.status_code in ( 200, ):
      raise AirflowException(f"Bad response from JID {resp}: {resp.content}")
    context['ti'].xcom_push( key='return_value', value=resp.content )
    
class PutFileOperator( JIDFileBase ):

  @apply_defaults
  def __init__(self,
    filepath,
    data,
    *args, **kwargs):
    super( PutFileOperator,  self ).__init__(*args, **kwargs)
    self.filepath = filepath
    self.data = data

  def execute( self, context ):
    uri = self.get_uri()
    LOG.info( f"Calling {uri}..." )
    resp = requests.put( uri, data=self.data, **self.requests_opts )
    v = self.parse( resp )
    if 'status' in v and v['status'] == 'ok':
      return True
    return False
    

class JIDPlugins(AirflowPlugin):
    name = 'jid_plugins'
    operators = [LsSensor,GetFileSensor,PutFileOperator,JIDJobOperator,JIDOperator,]
