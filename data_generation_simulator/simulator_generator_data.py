import psycopg2
import pandas as pd
import psycopg2.extras
from datetime import datetime,timezone
from config import load_config

def connect(config):
    """ Connect to the PostgreSQL database server """
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgreSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def query_sample_data(conn,query):
    try :
         with conn.cursor() as cur: # Open on cursor to fetch data from database
            cur.execute(query)
            # print("The number of parts: ", cur.rowcount)
            row = cur.fetchone()
            # while row is not None:
            #     print(row)
            #     row = cur.fetchone()
            cur.close()
            return row[0]
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def load_data():
    order_insert_data = pd.read_csv('dataset/order_insert_stg.csv')
    order_product_insert_data = pd.read_csv('dataset/order_product_insert_stg.csv')
    return order_insert_data, order_product_insert_data # Return 2 dataframe

def insert(conn,table,df): #table: table name, df: dataframe, conn: connection to postgreSQL
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query = "INSERT INTO %s(%s) VALUES %%s" % (table,cols) # %% symbol use to make ineffective %s in query text

    try :
         with conn.cursor() as cur: # Open on cursor to fetch data from database
            psycopg2.extras.execute_values(cur,query,tuples)
            conn.commit()
            print("Insert completely")
            cur.close()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


if __name__ == '__main__':
    config = load_config()
    conn = connect(config)
    # text = query_sample_data(conn,'select last_value from order_stg_order_id_seq')
    order_insert_data, order_product_insert_data = load_data()

    for i in range(0,2): #len(order_insert_data)):
        ''' Insert data from table order_stg '''
        print('Table: order_stg')
        df_insert = order_insert_data.iloc[i:i+1,:]

        # Exclude 2 col order_id, order_code
        df_insert = df_insert.drop(['order_id', 'order_code'], axis=1)

        # Add 2 col timestamp 
        df_insert['created_at'] = datetime.now(timezone.utc) .isoformat(sep=" ")
        df_insert['last_updated_at'] = datetime.now(timezone.utc) .isoformat(sep=" ")

        # insert data into table
        insert(conn,"order_stg",df_insert)

        ''' Insert data from table order_product_stg '''
        print('Table: order_product_stg')
        df_insert = order_product_insert_data.iloc[i:i+1,:]

        # Exclude 2 col order_id, order_code
        df_insert = df_insert.drop(['order_id', 'order_code'], axis=1)

        # Add 4 col timestamp
        last_order_id = query_sample_data(conn,'select last_value from order_stg_order_id_seq')
        df_insert['order_id'] = last_order_id
        df_insert['created_at'] = datetime.now(timezone.utc) .isoformat(sep=" ")
        df_insert['last_updated_at'] = datetime.now(timezone.utc) .isoformat(sep=" ")

        # insert data into table
        insert(conn,"order_product_stg",df_insert)
