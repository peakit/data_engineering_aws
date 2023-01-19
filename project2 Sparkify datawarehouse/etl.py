import os
from pathlib import Path
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from create_tables import get_or_create_redshift, get_or_create_iam_role

path = Path(__file__)
ROOT_DIR = path.parent.absolute()
config_path = os.path.join(ROOT_DIR, "dwh.cfg")

config = configparser.ConfigParser()
config.read(config_path)

ROLE_ARN = get_or_create_iam_role(config)['Role']['Arn']
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        query = query.format(log_data=LOG_DATA, 
                    song_data=SONG_DATA,
                    role_arn=ROLE_ARN,
                    log_jsonpath=LOG_JSONPATH
                    )
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # use the redshift cluster identifier and find the endpoint

    cluster_props = get_or_create_redshift(config)
    cluster_endpoint_address = cluster_props['Clusters'][0]['Endpoint']['Address']

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
                            cluster_endpoint_address,
                            config.get('CLUSTER','DB_NAME'),
                            config.get('CLUSTER', 'DB_USER'),
                            config.get('CLUSTER', 'DB_PASSWORD'),
                            config.get('CLUSTER', 'DB_PORT'))
                        )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
