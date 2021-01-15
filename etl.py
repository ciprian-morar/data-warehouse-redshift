import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''This function executes insert staging tables queries
    Parameters:
            cur (object): object cursor
            conn (object): object conn
    Returns
            None
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''This function executes insert star schema tables queries
    Parameters:
            cur (object): object cursor
            conn (object): object conn
    Returns
            None
    '''
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    '''The script connect to our AWS Redshift cluster 
    and perform etl operations to load and insert the data in staging tables or star schema tables.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("before connection")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("after connection")
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()