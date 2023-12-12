import pytest
import sys
import os
from sqlalchemy_proj8 import validate_artist_df
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

PATH_ERROR= os.getenv("PATH_ERROR")


def test_validate_artist_df():
   
    columns = ["DisplayName", "ArtistBio", "Nationality", "Gender", "BeginDate", "EndDate"]
    data = {
        "DisplayName": ["John", "Jane", "Bob"],
        "ArtistBio": ["Bio1", "Bio2", "Bio3"],
        "Nationality": ["US", "UK", "CA"],
        "Gender": ["Male", "Female", "Male"],
        "BeginDate": [1990, 1985, 2000],
        "EndDate": [2020, 2022, 2010]
    }
    df = pd.DataFrame(data, columns=columns)

    result_df = validate_artist_df(df)

    assert len(result_df) == 3  
    assert all(result_df.dtypes == [object, object, object, object, int, int])

    with open('errors.log', 'r') as log_file:
        log_contents = log_file.read()
        assert log_contents.strip() == ""

    data = {
    "DisplayName": ["John", "Jane", "Bob"],
    "ArtistBio": ["Bio1", "Bio2", "Bio3"],
    "Nationality": ["US", "UK", "CA"],
    "Gender": ["Male", "Female", 2],
    "BeginDate": [1000,"Avengers", 2000],
    "EndDate": [2020, 2022, 2010]
    }
    df = pd.DataFrame(data, columns=columns)

    result_df = validate_artist_df(df)
    assert len(result_df) == 1 
    assert all(result_df.dtypes == [object, object, object, object, int,int])  

    with open('errors.log', 'r') as log_file:
        log_contents = log_file.read()
        assert log_contents.strip() != ""