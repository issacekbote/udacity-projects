import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    The purpose of this function is to load the data into staging 
    tables from S3 storage by calling copy command within sql_queries
    module. This function is called by the main function. 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function loads the data from staging server to fact and
    dimension tables by calling INSERT queries within sql_queries
    module. The function is called by main function
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is main function of this module
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} \
            port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

