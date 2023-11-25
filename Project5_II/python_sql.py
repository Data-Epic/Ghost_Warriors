import os
import gspread
import logging
import psycopg2
from dotenv import load_dotenv

load_dotenv()
#configure logging library
logging.basicConfig(filename='errors.log', level=logging.ERROR,
                    format = '%(levelname)s (%(asctime)s) : %(message)s (%(lineno)d)')

def setup():
    """authorizes gspread and gets data
    from spreadsheet"""
    #get current working directory
    current_working_directory = os.getcwd()
    #get environment variable project_json
    project_json = os.getenv("PROJECT_JSON")
    path = os.path.join(current_working_directory,project_json)

    #authenticate gspread
    client = gspread.service_account(filename = path)
    #connect to online spreadsheet
    spreadsheet = client.open("Gspread SpreadSheet")
    #access worksheet in spreadsheet
    worksheet = spreadsheet.worksheet("Sheet1")
    #get all values from worksheet
    github_data = worksheet.get_all_values()
    #github_data[1][0] = 1
    
    return github_data

# Connect to an existing database
def validate_data(github_data):
    """function checks that each
    element in each row is of expected 
    datatype """
    data = []

    #loop through data and check if each is of expected data type
    for num in range(1,len(github_data)):
        if (isinstance(github_data[num][0], str) and
            isinstance(github_data[num][1], str) and
            isinstance(github_data[num][2], str) and
            isinstance(github_data[num][3], str)):
            
            data.append(github_data[num])
        else:
            #log error message to error.log file
            logging.error(f"Invalid Data Type in row")
            
    return data


def database_connect_execute():
    """connects to postgres database
    and executes sql commands to perform 
    operations"""
    #get password from environment variable
    PASSWORD = os.getenv("PASSWORD")
    #connect to postgres database
    with psycopg2.connect(host="localhost",dbname="GspreadData_Storage",
                        user="postgres",password=PASSWORD,port=5432) as conn:

    # Open a cursor to perform database operations
        with conn.cursor() as cur:

            #sql function to crreate table if it doesn't exist
            createTable = """CREATE TABLE IF NOT EXISTS Github_Records(
                ID SERIAL NOT NULL,
                Repository_Name VARCHAR(100) NOT NULL,
                Language_Used VARCHAR(50),
                Description VARCHAR(200),
                Datetime_Posted VARCHAR(50) NOT NULL,
                PRIMARY KEY (ID)
            )"""

            cur.execute(createTable)
            #sql function to clear table, so that on each run table will be cleared first
            truncate_table = """TRUNCATE TABLE Github_Records RESTART IDENTITY;"""
            cur.execute(truncate_table)
            #sql command to insert values into table
            insert_into_table = """INSERT INTO Github_Records(Repository_Name,
                                                            Language_Used, Description,
                                                            Datetime_Posted)
            
                                VALUES(%s,%s,%s,%s)"""
            github_data = setup()
            #calling validation function
            data = validate_data(github_data)
            for num in range(len(data)):
                cur.execute(insert_into_table,tuple(data[num]))


if __name__ == "__main__":
    database_connect_execute()