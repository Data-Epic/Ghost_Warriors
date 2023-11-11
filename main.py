import gspread
from pandas import json_normalize
from dotenv import load_dotenv
import os
import logging
import requests
import json
import datetime as dt

def main():
    # Access keys using dotenv
    
    GSPREAD_KEY = os.environ.get["api_key"]
    WEATHER_API = os.environ.get["weather_api"]

    # Create a connection to the Google Sheets
    gc = gspread.service_account(GSPREAD_KEY)
    spreadsheet = gc.open("Gspread practice")

    logging.basicConfig(level=logging.INFO)

    with open(WEATHER_API) as json_data_file:
        config = json.load(json_data_file)

    payload = {'Key': config['Key'], 'q' : "berlin", 'aqi': 'no'}
    r = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)

    df = pass_json_into_df(r)

    populate_worksheet("Weather", spreadsheet, df)
    return spreadsheet, r


def pass_json_into_df(r):
    '''
    This function normalize the json file, converts the epoch to timestamp, rename the column 
    and select a few column to return from the normalized into a dataframe

    :param r: the json request gotten from the Weather API
    
    '''
    r_string = r.json()
    normalized = json_normalize(r_string)
    normalized['timestamp'] = normalized['location.localtime_epoch'].apply(lambda s : dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M'))
    normalized.rename(columns={'location.name': 'location', 
      'location.region': 'state',
      'location.country': 'country',
      'current.temp_c': 'temp_c',
      'current.wind_kph': 'wind_kph',
      'location.lat': 'latitude',
      'location.lon' : 'longitude',
      'current.wind_dir': 'wind_direction',
      'currrent.wind_degree': 'wi_degree',
      'location.localtime_epoch': 'Localtime_epoch'
      }, inplace=True)  
    
    df = normalized.filter(['timestamp','location','state','country', 'temp_c','wind_kph', 'latitude', 'longitude', 'wind_direction' ])
    return df 


def get_or_create_worksheet(spreadsheet, ecom , rows=1, cols=1):
    '''
    This function gets a worksheet(Ecom) and if not found, it creates the worksheet

    :param spreadsheet: This is the spreadsheet which the worksheet is gotten from.
    :param ecom: This is the worksheet to get or create
    :param rows: This is the no of row initially created 
    :param col: This is the no of column initially created
    :return: The retrieved or newly created worksheet
    '''
    try:
        worksheet = spreadsheet.worksheet(ecom)
        logging.info("This worksheet already exist")
    except:
        worksheet = spreadsheet.add_worksheet(ecom, rows, cols)
        logging.info("The worksheet was not found, a new one will be created")
    return worksheet

def populate_worksheet(ecom, spreadsheet, df):
    '''
    This function appends the values of a dataframe to the next available row in a worksheet.
    
    :param ecom: The title of the worksheet to populate.
    '''
    worksheet = get_or_create_worksheet(spreadsheet,ecom)
    # Find the next available row for data insertion.
    next_row = len(worksheet.get_all_values()) + 1  # +1 to append to the next row
    
    # If the next available row exceeds the current rows of the worksheet,this will resize resize the worksheet.
    if next_row + len(df) > worksheet.row_count:
        worksheet.resize(rows=(next_row + len(df)))
    
    # If it's the very first insertion, include the headers.
    if next_row == 1:
        worksheet.update('A1', [df.columns.tolist()] + df.values.tolist())
    else:
        worksheet.update(f'A{next_row}', df.values.tolist())
    
    logging.info("The data has been successfully loaded into the spreadsheet")


if __name__ == "__main__":
    main()
