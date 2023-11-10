from main import get_or_create_worksheet
from main import pass_json_into_df
from main import main

def test_for_successful_worksheet_creation():
    '''
    This test for the spreadsheet title and worksheet title, it also test to see if there are values in the worksheet
    '''
    spreadsheet,r = main()
    worksheet_name = "Weather"
    worksheet = get_or_create_worksheet( spreadsheet, 'Weather')
    
    assert worksheet is not None
    assert worksheet.title == worksheet_name
    assert spreadsheet.title == 'Gspread practice'
    assert worksheet.row_count != 0

def test_for_column_names():
    '''
    This test for the if the right column names is present in the columns of the dataframe
    '''
    spreadsheet, r = main()
    df = pass_json_into_df(r)
    expected_columns = ['timestamp', 'location', 'state', 'country', 'temp_c', 'wind_kph', 'latitude', 'longitude', 'wind_direction']
    assert len(df.columns) == len(expected_columns)
    assert "location" in df.columns, "Missing column 'location'"
    assert "state" in df.columns, "Missing column 'state'"
    assert "country" in df.columns, "Missing column 'country'"
    assert "timestamp" in df.columns, "Missing column 'timestamp'"
    assert 'wind_direction' in df.columns, "Missing column 'wind_direction'"
    assert 'temp_c' in df.columns, "Missing column 'temp_c'"
    assert 'wind_kph' in df.columns, "Missing column 'wind_kph'"
    assert 'latitude' in df.columns, "Missing column 'latitude'"
    assert 'longitude' in df.columns, "Missing column 'longitude'"
    assert 'wind_direction' in df.columns, "Missing column 'wind_direction'"

def test_for_column_datatypes():
    '''
    This test checks the data types of columns in the DataFrame.
    '''
    spreadsheet , r = main()
    df = pass_json_into_df(r)
    assert df['location'].dtype == 'object', "Column 'location' should have data type 'object'"
    assert df['state'].dtype == 'object', "Column 'state' should have data type 'object'"
    assert df['country'].dtype == 'object', "Column 'country' should have data type 'object'"
    assert df['timestamp'].dtype == 'object', "Column 'timestamp' should have data type 'object'"
    assert df['wind_direction'].dtype == 'object', "Column 'wind_direction' should have data type 'object'"
    assert df['temp_c'].dtype == 'float64', "Column 'temp_c' should have data type 'float64'"
    assert df['wind_kph'].dtype == 'float64', "Column 'wind_kph' should have data type 'float64'"
    assert df['latitude'].dtype == 'float64', "Column 'latitude' should have data type 'float64'"
    assert df['longitude'].dtype == 'float64', "Column 'longitude' should have data type 'float64'"

def test_for_data_validation():
    '''
    This test checks if data in certain columns falls within valid ranges
    '''
    spreadsheet, r = main()
    df = pass_json_into_df(r)
    assert (df['temp_c'] >= -100).all(), "Invalid temperature values"
    assert (df['temp_c'] <= 100).all(), "Invalid temperature values"
    assert (df['wind_kph'] >= 0).all(), "Invalid wind speed values"
    assert (df['wind_kph'] <= 300).all(), "Invalid wind speed values"