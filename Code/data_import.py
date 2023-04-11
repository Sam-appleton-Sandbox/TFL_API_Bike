import psycopg2
import pandas as pd
import Nested_to_csv

def create_database():

    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=samappleton password=pandora12")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    #create sparkify database with UTF-8 encoding.
    cur.execute("DROP DATABASE api")
    cur.execute("CREATE DATABASE api")
    
    #close connection to default database
    conn.close()

    #connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=api user=samappleton password=pandora12")
    cur = conn.cursor()

    return cur, conn

def drop_tables(cur,con):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur,con):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()



def main():  
    try:
        cur, conn = create_database()





        #Cleans Api 
        api_col = pd.read_csv(Nested_to_csv.api_path)
        api_col = api_col.drop(['$type','casualties','vehicles','Unnamed: 0'], axis=1)

        #Cleans Vehicle
        api_col2 = pd.read_csv(Nested_to_csv.api_vehicle_path)
        api_col2 = api_col2.drop(['$type','Unnamed: 0'], axis=1)


        #Cleans Casualty
        api_col3 = pd.read_csv(Nested_to_csv.api_casualty_path)
        api_col3 = api_col3.drop(['$type','Unnamed: 0'], axis=1)
        print(api_col3.columns)





        api_table_create = ("""CREATE TABLE IF NOT EXISTS api(
        id INT PRIMARY KEY,
        lat INT,
        lon INT,
        location VARCHAR,
        date TIMESTAMP,
        severity VARCHAR,
        borough VARCHAR
        )""")


        cur.execute(api_table_create)
        conn.commit()

        api_vehicle_table_create = ("""CREATE TABLE IF NOT EXISTS api_vehicle(
        type VARCHAR,
        id INT


        )""")


        cur.execute(api_vehicle_table_create)
        conn.commit()

        api_casualty_table_create = ("""CREATE TABLE IF NOT EXISTS api_casualty(
        age FLOAT,
        class VARCHAR,
        severity VARCHAR,
        mode VARCHAR,
        ageBand VARCHAR,
        id INT 
        )""")


        cur.execute(api_casualty_table_create)
        conn.commit()




        api_table_insert = (""" INSERT INTO api(
            id,
            lat,
            lon,
            location,
            date,
            severity,
            borough)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """)

        count = 0
        for i, row in api_col.iterrows():
            cur.execute(api_table_insert,list(row))
            count += 1
        conn.commit()
        print(f'The total amount of rows written to api was {count}')
        #adding data to vehicle table

        api_vehicle_table_insert = (""" INSERT INTO api_vehicle(
            type,
            id
            

            )
            VALUES (%s,%s)
        """)



        count = 0
        for i2, row2 in api_col2.iterrows():
            cur.execute(api_vehicle_table_insert,list(row2))
            count += 1
            
        conn.commit()
        print(f'The total amount of rows written to api_vehicle was {count}')

        #adding data to casualty table

        api_casualty_table_insert = (""" INSERT INTO api_casualty(
            age,
            class,
            severity,
            mode,
            ageBand,
            id )
            VALUES (%s,%s,%s,%s,%s,%s)
        """)

        count = 0
        for i3, row3 in api_col3.iterrows():
            cur.execute(api_casualty_table_insert,list(row3))
            count += 1
        conn.commit()
        print(f'The total amount of rows written to api_casualty was {count}')
    except Exception as e:
        print(e)





