import psycopg2
from config import config
import gspread
from dotenv import load_dotenv
import os
import logging

def connect_and_create_table():
    """
    This function connect to the PostgreSQL database server and creates a table(weather) if does not already exist
    """
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


def get_gspread_data():
    '''
    This function create a connection to the Google Sheets and creates a list of list of the data in the worksheet
    
    '''
    load_dotenv()
    GSPREAD_KEY = os.getenv('PROJECT_KEY')
    # Create a connection to the Google Sheets
    gc = gspread.service_account(GSPREAD_KEY)
    spreadsheet = gc.open("Gspread practice")
    logging.basicConfig(level=logging.INFO)
    worksheet = spreadsheet.worksheet('Weather')
    data = worksheet.get_all_records()
    return data 


def validation(data):
    """
    This function validates the datatype of each record before loading into PostgreSQL.

    :param data: This is the list of dictionary for the the data in the excel sheet

    """
    validated_data = []
    for record in data:
        try:
            
            valid_record = {
                'location': str(record['location']),
                'state': str(record['state']),
                'country': str(record['country']),
                'wind_direction': str(record['wind_direction']),
                'temp_c': float(record['temp_c']),
                'wind_kph': float(record['wind_kph']),
                'latitude': float(record['latitude']),
                'longitude': float(record['longitude']),
                'timestamp': record['timestamp'] 
            }
            validated_data.append(valid_record)
        except (ValueError, KeyError) as e:
            print(f"Skipping row due to error: {e}")
    return validated_data

def insert_into_postgres(validated_data):
    """ 
    THis function inserts data into PostgreSQL database. 

    :param validated_data: this is the validated data after the datatype confirmation
    """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        for record in validated_data:
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
    validated_data = validation(data)
    insert_into_postgres(validated_data)