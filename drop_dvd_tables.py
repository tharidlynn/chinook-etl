import os
import psycopg2

# this modified version added pg dvd rentals for some reasons
dvd_actor_drop = 'DROP TABLE IF EXISTS actor'
dvd_category_drop = 'DROP TABLE IF EXISTS category'
dvd_film_drop = 'DROP TABLE IF EXISTS film'
dvd_film_actor_drop = 'DROP TABLE IF EXISTS film_actor'
dvd_film_category_drop = 'DROP TABLE IF EXISTS film_category'

# query lists
drop_dvd_table_queries = [dvd_actor_drop, dvd_category_drop, dvd_film_drop, dvd_film_actor_drop, dvd_film_category_drop]

def drop_dvd_tables(cur, conn):
    for query in drop_dvd_table_queries:
        cur.execute(query)
        conn.commit()
        print('DROP TABLE')

def main():

    PG_HOST = os.environ.get('PGHOST')
    PG_USERNAME = os.environ.get('PGUSERNAME')
    PG_PASSWORD = os.environ.get('PGPASSWORD')
    PG_DATABASE = os.environ.get('PGDATABASE')
   
    # connect to chinook database
    conn = psycopg2.connect(host=PG_HOST, 
                            dbname=PG_DATABASE, 
                            user=PG_USERNAME,
                            password=PG_PASSWORD)
    cur = conn.cursor()
    
    try:
        drop_dvd_tables(cur, conn)
    except psycopg2.Error as e:
        print(e)
        
    conn.close()

if __name__ == '__main__':
    main()