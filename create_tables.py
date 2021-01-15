import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''This function drop the tables from the database
    Parameters:
            cur (object): object cursor
            conn (object): object conn
    Returns
            None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''This function create new tables in the database
    Parameters:
            cur (object): object cursor
            conn (object): object conn
    Returns
            None
    '''
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    '''The script connect to our AWS Redshift cluster and call drop 
    and create tables functions to create new emmpty tables
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("before connection")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("after connection")
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()