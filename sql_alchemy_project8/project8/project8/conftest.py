import pytest
import pandas as pd


@pytest.fixture(autouse=True)
def create_df_artist():
    columns = ["DisplayName", "ArtistBio", "Nationality", "Gender", "BeginDate", "EndDate"]
    data1 = {
        "DisplayName": ["John", "Jane", "Bob"],
        "ArtistBio": ["Bio1", "Bio2", "Bio3"],
        "Nationality": ["US", "UK", "CA"],
        "Gender": ["Male", "Female", "Male"],
        "BeginDate": [1990, 1985, 2000],
        "EndDate": [2020, 2022, 2010]
    }
    df1 = pd.DataFrame(data1, columns=columns)

    data2 = {
    "DisplayName": ["John", "Jane", "Bob"],
    "ArtistBio": ["Bio1", "Bio2", "Bio3"],
    "Nationality": ["US", "UK", "CA"],
    "Gender": ["Male", "Female", 2],
    "BeginDate": [1000,"Avengers", 2000],
    "EndDate": [2020, 2022, 2010]
    }
    df2 = pd.DataFrame(data2, columns=columns)

    return df1, df2

