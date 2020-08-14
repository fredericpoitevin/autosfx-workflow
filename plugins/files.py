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



class BulkFilesOperator( BaseOperator ):
  ui_color = '#b19cd9'
  #template_fields = ('',)

  locations = {
    'SLAC': "https://pslogin01:9443", 
  }
  endpoints = {
    'copy_paths': '/filetransfer/ws/copy_paths',
  }

  @apply_defaults
  def __init__(self, 
      experiment=None, 
      source='NERSC',
      dest='SLAC',
      paths=[],
      dry_run=False,
      run_at='SLAC',
      poke_interval=30,
      cert='/usr/local/airflow/dags/certs/airflow.crt', key='/usr/local/airflow/dags/certs/airflow.key', root_ca='/usr/local/airflow/dags/certs/rootCA.crt', xcom_push=True, 
      *args, **kwargs ):
    super(BulkFilesOperator, self).__init__(*args, **kwargs)
    if len(paths) == 0:
      raise Exception('No paths defined')
    self.experiment = experiment
    self.dry_run = dry_run
    self.source = source
    self.dest = dest
    self.paths = paths
    self.poke_interval = poke_interval
    self.run_at = run_at
    self.requests_opts = {
      "cert": ( cert, key ),
      "verify": False,
    }


  def create_control_doc( self, context ):
    def_id = 'anything'
    return { 
      'experiment': self.experiment,
      'dry_run': self.dry_run,
      'source': self.source,
      'dest': self.dest, 
      'paths': self.paths,
    }
    
  def rpc( self, endpoint, control_doc ):

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

    LOG.info(f"RUN AT {self.run_at}")
    if self.run_at != 'SLAC':
      raise Exception(f'Unregistered control point {self.run_at}')

    control_doc = self.create_control_doc( context )

    msg = self.rpc( 'copy_paths', control_doc )
    LOG.info(f"GOT files {msg}")
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


class FilesPlugins(AirflowPlugin):
    name = 'files_plugins'
    operators = [BulkFilesOperator,]
