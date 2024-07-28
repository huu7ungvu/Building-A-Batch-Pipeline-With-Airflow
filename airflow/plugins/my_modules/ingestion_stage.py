from google.cloud.sql.connector import Connector, IPTypes
from google.cloud import storage
import sqlalchemy
from configparser import ConfigParser
import pandas as pd
from datetime import datetime,timezone
import fastavro
from pathlib import Path
from json import load as jsload
from config import load_config

base_dir = Path(__file__).resolve().parent.parent
avro_filepath = base_dir / 'my_modules'/ 'table_schema_avro.json'

# def load_config(filename=ini_filepath, section='GSQL'):
#     # print(os.path.abspath(__file__))
#     parser = ConfigParser()
#     parser.read(filename)

#     # get section, default to postgresql
#     config = {}
#     if parser.has_section(section):
#         params = parser.items(section)
#         for param in params:
#             config[param[0]] = param[1]
#     else:
#         raise Exception('Section {0} not found in the {1} file'.format(section, filename))

#     # return config
#     # init parameters
#     instance_connection_name = f"{config['project_id']}:{config['region']}:{config['instance_name']}"
#     return instance_connection_name, config

def get_schema_avro_file(table_name):
    with open (avro_filepath,'r') as file:
    # Load the JSON
        schema = jsload(file)

    return schema[table_name]

def get_con():
    connector = Connector()
    config = load_config()
    instance = f"{config['project_id']}:{config['region_1']}:{config['instance_name']}"
    db_user,db_pass,db_name = config['db_user'], config['db_pass'], config['db_name']

    # set connection params
    conn = connector.connect(
        instance,
        "pg8000",
        user = db_user,
        password = db_pass,
        db = db_name,
        ip_type = IPTypes.PUBLIC
    )

    return conn

def get_last_updated_ts(table_name, config):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(config['gcs_bucket_name'])
        blob = bucket.blob('log/{0}_log-00000-of-00001.txt'.format(table_name))
        last_updated_at = blob.download_as_text()
        return last_updated_at
    except:
        return None

def capture_data_change(pool,table_name,config):
    # get last_updated_ts of product table
    last_updated_at = get_last_updated_ts(table_name,config)

    # capture data
    capture_data_change_df = pd.read_sql_query(
        "select * from {0} where last_updated_at > '{1}'".format(table_name, last_updated_at),
        con=pool
    ) if last_updated_at is not None else pd.read_sql_query(
        "select * from {0}".format(table_name),
        con=pool
    )

    # Replace None object value to blank string
    capture_data_change_df = capture_data_change_df.fillna('')

    # print number of records capture from db
    print("Number records captured: {0}".format(len(capture_data_change_df)))

    # get list cols
    list_cols = list(capture_data_change_df.columns)
     
    # adding ingested at col
    capture_data_change_df['ingested_at'] = datetime.now(timezone.utc).isoformat(sep=" ")

    # reorder cols
    list_cols.insert(0,'ingested_at')
    capture_data_change_df = capture_data_change_df[list_cols]

    # change type cols
    capture_data_change_df['created_at'] = capture_data_change_df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    capture_data_change_df['last_updated_at'] = capture_data_change_df['last_updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # create schema for avro file
    schema_data_tb = get_schema_avro_file(table_name)
    schema = fastavro.parse_schema(schema_data_tb)

    # generate the avro file
    records = capture_data_change_df.to_dict('records')
    filename = "{0}_capture_change_data.avro".format(table_name)
    with open(filename,'wb') as file:
        fastavro.writer(file,schema,records)
    print("Created avro file successfully")

def push_gcs(table_name,config):
    storage_client = storage.Client()
    bucket = storage_client.bucket(config['gcs_bucket_name'])
    filepath = "push_cdc/{0}_capture_change_data.avro".format(table_name)
    filename = "{0}_capture_change_data.avro".format(table_name)
    blob = bucket.blob(filepath)

    # generation_match_precondition = 1
    blob.upload_from_filename(
        filename
        # if_generation_match = generation_match_precondition
    )
    print("Push avro file to GCS successfully")

def ingest_data(table_name):
    print("Start data ingestion stage")
    config = load_config()
    # create pool engine
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=get_con, # Requirement: this func does not have params 
    )
    # cdc data 
    capture_data_change(pool,table_name,config)
    # push data to GCS
    push_gcs(table_name,config)

    print("Ingest data successfully")