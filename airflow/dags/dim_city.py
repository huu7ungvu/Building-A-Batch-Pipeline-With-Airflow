import datetime
import pendulum
import os
from pathlib import Path

import airflow
from airflow.decorators import dag, task
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import apache_beam as beam 

from my_modules.ingestion_stage import ingest_data
import my_modules.transformation_load_stage as mo

# global var
db_table_name = 'city_location_stg'
dw_table_name = 'dim_city'

@dag(
    dag_id="{0}_etl_daily".format(dw_table_name),
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["prj"]
)

def elt_dim_city():
    @task  
    def ingest_data_task():
        ingest_data(db_table_name)
    
    @task
    def transform_and_load_task():
        print("Start data transformation and load stage")
        # config set up file
        base_dir = Path(__file__).resolve().parent.parent
        filepath = base_dir /'plugins' / 'setup.py' # not return a string

        # init some var for creation beam pipeline
        avro_path_file_gcs = "gs://ingestion_layer/push_cdc/{0}_capture_change_data.avro".format(db_table_name)
        table_path_bigquery = "liquid-kite-423215-s2.dw_demo.{0}".format(dw_table_name)
        schema_table_bigquery = 'ingested_at:TIMESTAMP\
            ,created_at:TIMESTAMP\
            ,last_updated_at:TIMESTAMP\
            ,city_id:INTEGER\
            ,city_name:STRING\
            ,country_id:INTEGER\
            '
        dedup_unique_key = 'city_id'  

        # config beam pipeline
        options = PipelineOptions(save_main_sesion = True, setup_file = str(Path(filepath)), pickle_library = 'cloudpickle')
        gcp_options = options.view_as(GoogleCloudOptions)
        gcp_options.dataflow_endpoint = 'https://dataflow.googleapis.com'
        gcp_options.project = 'liquid-kite-423215-s2'
        gcp_options.job_name = "{0}-bigquery-etl".format(dw_table_name.replace("_","-"))
        gcp_options.service_account_email = 'dataflow@liquid-kite-423215-s2.iam.gserviceaccount.com'
        gcp_options.region = 'asia-southeast2'
        gcp_options.staging_location = 'gs://ingestion_layer/staging'
        gcp_options.temp_location = 'gs://ingestion_layer/tmp'

        options.view_as(StandardOptions).runner = 'DataflowRunner'
        
        # transform data
        with beam.Pipeline(options=options) as p:
            # (
            #     p
            #     | 'Start' >> beam.Create([None])
            #     | 'ReadAvro' >> mo.ReadAvroFromGCS(avro_path_file_gcs)
            #     | 'RemoveDuplicates' >> mo.RemoveDuplicates(dedup_unique_key)
            #     | 'WriteToBigQuery' >> mo.PushToBigQuery(table_path_bigquery, schema_table_bigquery)
            # )
            read_convert_remove = ( 
                p
                | 'Start' >> beam.Create([None])
                | 'ReadAvro' >> mo.ReadAvroFromGCS(avro_path_file_gcs)
                | 'RemoveDuplicates' >> mo.RemoveDuplicates(dedup_unique_key)
            )
            write_to_bigquery = ( 
                read_convert_remove
                | 'WriteToBigQuery' >> mo.PushToBigQuery(table_path_bigquery, schema_table_bigquery)
            )

            get_max_last_updated_value = (
                read_convert_remove 
                | 'ExtractDateString' >> beam.Map(lambda element: element['last_updated_at'])
                | 'FindMaxDateString' >> beam.CombineGlobally(mo.MaxDateFn())
                | 'WriteLog' >> mo.WriteLogGCS('gs://ingestion_layer/log/{0}_log'.format(db_table_name))
            )

            result = p.run()
            result.wait_until_finish()
        
        print("Transform and load stage successfully")

    # create airflow pipeline
    ingest_data_task() >> transform_and_load_task() 

# create dag
dag = elt_dim_city()