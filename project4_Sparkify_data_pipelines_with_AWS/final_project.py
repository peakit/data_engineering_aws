from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    max_active_runs=1
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='myredshiftuser',
        aws_credentials_id='myawsuser',
        table='public.staging_events',
        s3_bucket='udacity-dend',
        s3_key='log-data/'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id = 'myredshiftuser',
        aws_credentials_id = 'myawsuser',
        table = 'public.staging_songs',
        s3_bucket = 'udacity-dend',
        s3_key = 'song-data/'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='myredshiftuser',
        query=final_project_sql_statements.SqlQueries.songplay_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='myredshiftuser',
        truncate_query=final_project_sql_statements.SqlQueries.song_table_truncate,
        insert_query=final_project_sql_statements.SqlQueries.song_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='myredshiftuser',
        truncate_query=final_project_sql_statements.SqlQueries.user_table_truncate,
        insert_query=final_project_sql_statements.SqlQueries.user_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='myredshiftuser',
        truncate_query=final_project_sql_statements.SqlQueries.artist_table_truncate,
        insert_query=final_project_sql_statements.SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='myredshiftuser',
        truncate_query=final_project_sql_statements.SqlQueries.time_table_truncate,
        insert_query=final_project_sql_statements.SqlQueries.time_table_insert
    )

    data_quality_checks = [
        {'check_sql': """SELECT COUNT(DISTINCT songid) 
                           FROM public.songs
                      """, 
         'expected_result': 384542
         },
        {'check_sql': """SELECT COUNT(DISTINCT song_id) 
                           FROM public.staging_songs
                      """, 
         'expected_result': 384542
         }
    ]
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='myredshiftuser',
        checks=data_quality_checks
    )

    end_operator = DummyOperator(task_id='End_execution')

    # Setting dependency graph
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]   
    [stage_events_to_redshift, stage_songs_to_redshift]  >> load_songplays_table
    load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator
    

final_project_dag = final_project()