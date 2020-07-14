from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

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

  def _setup( **kwargs ):
    print( kwargs['dag_run'].run_id )

  setup = PythonOperator( task_id='setup',
    python_callable=_setup,
    provide_context=True
  )


  peaks = ValueSensor( task_id='peaks' )

  unitcell = JIDOperator( task_id='unitcell')
  unitcell2 = JIDOperator( task_id='unitcell2')

  merging = JIDOperator( task_id='merging')

  phasing = JIDOperator( task_id='phasing' )

  unitcell_file = FileSensor( task_id='unitcell_file' )

  fasta_file = FileSensor( task_id='fasta_file' )
  mtz_file = FileSensor( task_id='mtz_file' )


  unitcell >> peaks
  unitcell2 >> peaks

  peaks >> merging

  unitcell_file >> merging >> phasing

  merging >> mtz_file
  fasta_file >> phasing
  mtz_file >> phasing
