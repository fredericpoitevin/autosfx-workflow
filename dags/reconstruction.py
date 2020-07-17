import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.bash_operator   import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
#from reconstruction_subdag import consensus_cell_subdag


##### TEMPORARY FILE DEFINITIONS (should not be hard coded...) 
base_path = '/usr/local/airflow/'
exp_name = 'cxic0415'
exp_path = 'dags/data/${exp_name}/'
fasta_filepath  = 'test.fasta'
stream_filepath = 'cxic0415.stream'
mtz_filepath    = 'test.mtz'
map_filepath    = 'test.map'
pdb_filepath    = 'test.pdb'
nb_filepath     = 'test.ipynb'
nb_publishedpath= 'test.published'
#####


class FileSensor( BashOperator ):
  ui_color = '#b19cd9'

class ValueSensor( DummyOperator ):
  ui_color = '#CDEB8B'

class JIDOperator( BashOperator ):
  ui_color = '#006699'

dag_name = os.path.splitext(os.path.basename(__file__))[0]

default_args = {
        'start_date': datetime( 2020,1,1 ),
}

dag = DAG(
    dag_name,
    default_args=default_args,
    description='AutoSFX reconstruction DAG',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=900,
  )

##### FILESENSORS

fasta_file = FileSensor( task_id='fasta_file',
    bash_command="""
{% set filepath = params.base_path + '/' + params.exp_path + '/' + params.file %}
ls "{{ filepath }}" && [[ -f "{{ filepath }}" ]] && exit 0
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path,
      'file': fasta_filepath },
    dag=dag,
  )

stream_file = FileSensor( task_id='stream_file',
    bash_command="""
{% set filepath = params.base_path + '/' + params.exp_path + '/' + params.file %}
ls "{{ filepath }}" && [[ -f "{{ filepath }}" ]] && exit 0
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path,
      'file': stream_filepath },
    dag=dag,
  )

mtz_file = FileSensor( task_id='mtz_file',
    bash_command="""
{% set filepath = params.base_path + '/' + params.exp_path + '/' + params.file %}
ls "{{ filepath }}" && [[ -f "{{ filepath }}" ]] && exit 0
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path,
      'file': mtz_filepath },
    dag=dag,
  )

map_file = FileSensor( task_id='map_file',
    bash_command="""
{% set filepath = params.base_path + '/' + params.exp_path + '/' + params.file %}
ls "{{ filepath }}" && [[ -f "{{ filepath }}" ]] && exit 0
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path,
      'file': map_filepath },
    dag=dag,
  )

pdb_file = FileSensor( task_id='pdb_file',
    bash_command="""
{% set filepath = params.base_path + '/' + params.exp_path + '/' + params.file %}
ls "{{ filepath }}" && [[ -f "{{ filepath }}" ]] && exit 0
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path,
      'file': pdb_filepath },
    dag=dag,
  )

nb_file = FileSensor( task_id='nb_file',
    bash_command="""
{% set filepath = params.base_path + '/' + params.exp_path + '/' + params.file %}
ls "{{ filepath }}" && [[ -f "{{ filepath }}" ]] && exit 0
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path,
      'file': nb_filepath },
    dag=dag,
  )

nb_published = FileSensor( task_id='nb_published',
    bash_command="""
{% set filepath = params.base_path + '/' + params.exp_path + '/' + params.file %}
ls "{{ filepath }}" && [[ -f "{{ filepath }}" ]] && exit 0
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path,
      'file': nb_publishedpath },
    dag=dag,
  )

##### PROCESS DEFINITIONS

merging = JIDOperator( task_id='merging',
    bash_command="""
cd {{ params.base_path }}/{{ params.exp_path }}
bash stream2mtz-test.sh
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path
    },
    dag=dag,
  )

phasing = JIDOperator( task_id='phasing',
    bash_command="""
cd {{ params.base_path }}/{{ params.exp_path }}
bash phasing.sh
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path
    },
    dag=dag,
  )

nb_create = JIDOperator( task_id='nb_create',
    bash_command="""
cd {{ params.base_path }}/{{ params.exp_path }}
bash nb_create.sh
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path
    },
    dag=dag,
  )

nb_publish = JIDOperator( task_id='nb_publish',
    bash_command="""
cd {{ params.base_path }}/{{ params.exp_path }}
bash nb_publish.sh
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path
    },
    dag=dag,
  )

elogs_results = JIDOperator( task_id='elogs_results',
    bash_command="""
cd {{ params.base_path }}/{{ params.exp_path }}
bash elogs_results.sh
""",
    params={
      'base_path': base_path,
      'exp_path': exp_path
    },
    dag=dag,
  )


#### DRAW THE DAG

stream_file >> merging >> mtz_file >> phasing >> map_file >> nb_create

fasta_file >> phasing >> pdb_file >> nb_create >> nb_file >> nb_publish >> nb_published

nb_published >> elogs_results

map_file >> elogs_results

pdb_file >> elogs_results



##### BELOW IS CODE TO BE RECYCLED

  #def _setup( **kwargs ):
  #  print( kwargs['dag_run'].run_id )
  #
  #setup = PythonOperator( task_id='setup',
  #  python_callable=_setup,
  #  provide_context=True
  #)
  #fasta_file      = FileSensor( task_id='fasta_file' )
  #runtag_file     = FileSensor( task_id='runtag_file' )
  #streamlist_file = FileSensor( task_id='streamlist_file' )
  #mtz_file        = FileSensor( task_id='mtz_file' )
  #map_file        = FileSensor( task_id='map_file' )
  #nb_file         = FileSensor( task_id='nb_file' )
  #consensus_cell = JIDOperator( task_id='consensus_cell' )
  #consensus_cell = SubDagOperator(
  #        task_id='consensus_cell',
  #        subdag=concensus_cell_subdag( os.path.splitext(os.path.basename(__file__))[0], 'consensus_cell'),
  #        dag=dag,
  #    )
  #merging = JIDOperator( task_id='merging' )
  #def _merging( **kwargs ):
  #  print('/gpfs/slac/cryo/fs1/daq/lcls/dev/airflow/dags/data/cxic0415/stream2mtz.sh')
  #merging = BashOperator( 
  #  task_id='merging',
  #  bash_command=_merging,
  #  dag=dag,
  #  )
  #phasing = JIDOperator( task_id='phasing' )
  #create_nb = JIDOperator( task_id='create_nb' )
  #publish_nb = JIDOperator( task_id='publish_nb' )
  #elog_diagnosis = JIDOperator( task_id='elog_diagnosis' )
  #elog_results   = JIDOperator( task_id='elog_results' )

  #runtag_file >> consensus_cell >> elog_diagnosis
  #consensus_cell >> streamlist_file >> merging 

