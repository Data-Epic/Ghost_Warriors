import psycopg2
from config import config
import gspread
from dotenv import load_dotenv
import os
import logging

def connect_and_create_table():
    """ Connect to the PostgreSQL database server"""
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        with open ('Project5/project5/create_table.sql', 'r') as file:
            create_table_sql = file.read()
        cur.execute(create_table_sql)
        conn.commit()
        print('Table created successfully')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#get the values from your gspread data and pass it into a df 
def get_gspread_data():
    load_dotenv()
    GSPREAD_KEY = os.getenv('PROJECT_KEY')
    # Create a connection to the Google Sheets
    gc = gspread.service_account(GSPREAD_KEY)
    spreadsheet = gc.open("Gspread practice")
    logging.basicConfig(level=logging.INFO)
    worksheet = spreadsheet.worksheet('Weather')
    data = worksheet.get_all_records()
    return data 

# validation function
def data_validation():
    pass

#insert into the columns with sql
def insert_into_postgres(data):
    """ Insert data into PostgreSQL database. """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        for record in data:
            values = (
                record['location'],
                record['state'],
                record['country'],
                record['wind_direction'],
                record['temp_c'],
                record['wind_kph'], 
                record['latitude'],
                record['longitude'],
                record['timestamp'] if record['timestamp'] else None
            )
            with open('Project5/project5/insert_values.sql', 'r') as file:
                insert_values_sql = file.read()
            cur.execute(
                insert_values_sql, values
            )
        conn.commit()
        print("Data inserted successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    connect_and_create_table()
    data = get_gspread_data()
    insert_into_postgres(data)