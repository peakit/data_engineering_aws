# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import pendulum

from airflow.decorators import dag,task
from airflow.models import Variable

from custom_operators.facts_calculator import FactsCalculatorOperator
from custom_operators.has_rows import HasRowsOperator
from custom_operators.s3_to_redshift import S3ToRedshiftOperator
#from airflow.operators.empty import EmptyOperator

#
# TODO: Create a DAG which performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
@dag(start_date=pendulum.now())
def full_pipeline():

    copy_trips_task = S3ToRedshiftOperator(
            task_id="copy_trips_task",
            redshift_conn_id="myredshiftuser", 
            aws_credentials_id="myawsuser", 
            table="trips",
            s3_bucket=Variable.get("s3_bucket"),
            s3_key="divvy/unpartitioned/divvy_trips_2018.csv")

    check_trips_task = HasRowsOperator(
            task_id="check_trips_task",
            redshift_conn_id="myredshiftuser",
            table="trips"
        )

    calculate_trips_fact_task = FactsCalculatorOperator(
            task_id="calculate_trips_fact_task",
            redshift_conn_id="myredshiftuser",
            origin_table="trips", 
            destination_table="tripduration_fact",
            fact_column="tripduration", 
            groupby_column="bikeid"
        )

    copy_trips_task >> check_trips_task
    check_trips_task >> calculate_trips_fact_task

full_pipeline_dag = full_pipeline()