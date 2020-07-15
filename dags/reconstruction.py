from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
#from reconstruction_subdag import consensus_cell_subdag

import os
from datetime import datetime

class FileSensor( DummyOperator ):
  ui_color = '#b19cd9'

class ValueSensor( DummyOperator ):
  ui_color = '#CDEB8B'

class JIDOperator( DummyOperator ):
  ui_color = '#006699'


# use name of file as name of DAG
with DAG( os.path.splitext(os.path.basename(__file__))[0],
  description="trail by firing squad",
  start_date=datetime( 2020,1,1 ),
  schedule_interval=None,
  catchup=False,
  max_active_runs=1,
  concurrency=1,
  dagrun_timeout=900,
  ) as dag:

  #def _setup( **kwargs ):
  #  print( kwargs['dag_run'].run_id )
  #
  #setup = PythonOperator( task_id='setup',
  #  python_callable=_setup,
  #  provide_context=True
  #)

  fasta_file      = FileSensor( task_id='fasta_file' )
  #runtag_file     = FileSensor( task_id='runtag_file' )
  streamlist_file = FileSensor( task_id='streamlist_file' )
  mtz_file        = FileSensor( task_id='mtz_file' )
  map_file        = FileSensor( task_id='map_file' )
  nb_file         = FileSensor( task_id='nb_file' )

  #consensus_cell = JIDOperator( task_id='consensus_cell' )
  #consensus_cell = SubDagOperator(
  #        task_id='consensus_cell',
  #        subdag=concensus_cell_subdag( os.path.splitext(os.path.basename(__file__))[0], 'consensus_cell'),
  #        dag=dag,
  #    )

  merging = JIDOperator( task_id='merging' )

  phasing = JIDOperator( task_id='phasing' )

  create_nb = JIDOperator( task_id='create_nb' )

  publish_nb = JIDOperator( task_id='publish_nb' )

  #elog_diagnosis = JIDOperator( task_id='elog_diagnosis' )
  elog_results   = JIDOperator( task_id='elog_results' )

  #runtag_file >> consensus_cell >> elog_diagnosis

  #consensus_cell >> streamlist_file >> merging 

  streamlist_file >> merging >> mtz_file >> phasing

  fasta_file >> phasing

  phasing >> map_file >> create_nb

  create_nb >> nb_file >> publish_nb

  publish_nb >> elog_results

