import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop any existing tables that need correcting. Start fresh."""
    print("*"*20)
    print("Starting Table Dropping")

    count= 1

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

        print(f"Finished {count} query(ies).")
        count+= 1

    print("Tables Dropped")


def create_tables(cur, conn):
    """Create tables for staging and analysis"""
    print("*"*20)
    print("Starting Table Creation")

    count= 1
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
        print(f"Finished {count} query(ies).")
        count+= 1

    print("Tables Created")


def main():
    """Loads configurations and runs drop_tables() and create_tables()"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Conn String: " + "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()