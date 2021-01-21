import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time

def load_staging_tables(cur, conn):
    """Load staging tables to Postgres DB hosted on AWS Redshift"""
    t= time()

    print("*"*20)
    print("Starting Copy")

    count= 1
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
        print(f"Finished {count} query(ies).")
        count+= 1

    print("Copy Completed")
    
    duration= "{:.1f}".format((time() - t)/60)
    print(f"Duration: {duration} minutes.")



def insert_tables(cur, conn):
    """Load data from staging tables and transform it to the appropriate Star-Schema tables"""
    t= time()

    print("*"*20)
    print("Starting Inserts")

    count= 1
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

        print(f"Finished {count} query(ies).")
        count+= 1

    print("Inserts Completed")
    
    duration= "{:.1f}".format((time() - t)/60)
    print(f"Duration: {duration} minutes.")

def main():
    """Loads configurations and runs load_staging_tables() and insert_tables()"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Conn String: " + "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    
    t_tot= time()

    main()

    print("*"*20)
    duration= "{:.1f}".format((time() - t_tot)/60)
    print(f"Completed in {duration} minutes.")