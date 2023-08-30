from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag, task

# Import Operatores & Hooks
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

# Import SQLs
from udacity.common.final_project_sql_statements import SqlQueries
from helpers.create_tables import SqlCreate

# DAG
@dag(
    default_args={'ulip': 'udacity','start_date': pendulum.now(), 'depends_on_past': False, 'retries': 3,
    'retry_delay': timedelta(minutes=5),'catchup': False, 'email_on_retry': False},
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    max_active_runs=3
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="uli-deng-usw2-final",
        s3_key="log-data",
        table_init = SqlCreate.staging_events_create,
        region = 'us-west-2',
        file_format="JSON",
        jsonpath = 's3://udacity-dend/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="uli-deng-usw2-final", 
        s3_key="song-data",
        table_init = SqlCreate.staging_songs_create,
        region = 'us-west-2',
        file_format="JSON",
        jsonpath='auto'
    )

    ####

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table = "songplays",
        sql= SqlQueries.songplay_table_insert,
        append_only=False,
        table_init  = SqlCreate.songplays_create
        
)

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="users",
        sql= SqlQueries.user_table_insert,
        append_only=False,
        table_init = SqlCreate.users_create
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="songs",
        sql= SqlQueries.song_table_insert,
        append_only=False,
        table_init = SqlCreate.songs_create
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="artists",
        sql= SqlQueries.artist_table_insert,
        append_only=False,
        table_init = SqlCreate.artists_create 
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="time",
        sql=SqlQueries.time_table_insert,
        append_only=False,
        table_init = SqlCreate.time_create
)

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        table=["songplays", "users", "songs", "artists", "time"]
)

    end_operator = DummyOperator(task_id='End_execution')


    # Schedule
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_songs_to_redshift  >> load_songplays_table
    stage_events_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_song_dimension_table   >> run_quality_checks
    load_user_dimension_table   >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table   >> run_quality_checks

    run_quality_checks >> end_operator

final_project_dag = final_project()