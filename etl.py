import psycopg2
import configparser
from prettytable import from_db_cursor
from sql_queries import copy_table_queries, insert_table_queries, analysis_queries



def load_staging_tables(cur, conn):
    """Load staging tables from .json file located on S3 bucket to Redshift."""
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data into final tables located on Redshift."""
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def analysis(cur, conn):
    """Run analysis queries related to top 10 artists and songs played."""
    
    for query in analysis_queries:
        cur.execute(query)
        conn.commit()
        table = from_db_cursor(cur) 
        print(table)
        
        
def main():
    """Main function
    load_staging_tables: Load staging tables from .json file located on S3 bucket to Redshift.
    insert_tables: Insert data into final tables located on Redshift.
    analysis: Run analysis queries related to top 10 artists and songs played.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    analysis(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
