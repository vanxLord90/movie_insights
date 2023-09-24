import psycopg2
from config import config
import constants_sql 
import pandas as pd

def connect():
    """Connect to postgresql """
    conn = None
    try:
        params = config()
        print ("Connecting to psql database")
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        print("PostgreSQL database version")
        cur.execute("Select version()")
        db_version = cur.fetchone()
        print(db_version)
        print("Unique Certificates in the content_recommender_db")

        # query_alter_type = pd.read_sql_query(constants_sql.ALTER_RATING_TYPE,conn)
        # query_genre = pd.read_sql_query(constants_sql.SQL_SELECT_UNIQUE_GENRE,conn)
        # query_stars = pd.read_sql_query(constants_sql.SQL_SELECT_UNIQUE_STARS,conn)

        with open("query_results.txt", "a") as f:
            
          print("We are awesome")
        f.close()
        #.execute()
        print("successful query")
        conn.commit()

        cur.close()


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed")

if __name__ == '__main__':
    connect()
