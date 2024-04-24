import os
import snowflake.connector
from snowflake.snowpark import Session
import time
import requests

print("STARTING...")
def connection(forceEgress=False) -> snowflake.connector.SnowflakeConnection:
    if os.path.isfile("/snowflake/session/token") and forceEgress==False:
        print("GOING FILE TOKEN")
        creds = {
            'host': os.getenv('SNOWFLAKE_HOST'),
            'port': os.getenv('SNOWFLAKE_PORT'),
            'protocol': "https",
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'authenticator': "oauth",
            'token': open('/snowflake/session/token', 'r').read(),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }
    else:
        print("GOING EGRESS")
        creds = {
            'account': "xxxxx.eu-west-1",
            'user': "me",
            'password': "",
            'client_session_keep_alive': True
        }

    connection = snowflake.connector.connect(**creds)
    return connection

def sessionpark(forceEgress=False) -> Session:
    return Session.builder.configs({"connection": connection(forceEgress)}).create()

sql = f''' 
         SELECT
            current_account() AS account,
            current_database() AS database,
            current_schema() AS schema
'''

def looper():
    print("-----REGULAR EGRESS TO GOOGLE)")
    r=requests.get('https://google.com',timeout=5)
    print(r)
    print("-----CONNECTOR Connection Using File token (if possible)")
    conn = connection()
    data = conn.cursor().execute(sql).fetch_pandas_all()
    print(data)
    print("-----")

    print("-----CONNECTOR Connection Using Egress")
    conn = connection(True)
    data = conn.cursor().execute(sql).fetch_pandas_all()
    print(data)
    print("-----")

    print("-----SNOWPARK Connection Using File token (if possible)")
    session = sessionpark()
    data = session.sql(sql).to_pandas()
    print(data)
    print("-----")

    print("-----SNOWPARK Connection Using Egress")
    session = sessionpark(True)
    data = session.sql(sql).to_pandas()
    print(data)
    print("-----")
    time.sleep(20)
    looper()


if __name__ == "__main__":
    looper()