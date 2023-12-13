import pytest
import sys
import os
from sqlalchemy_proj8 import validate_artist_df
import pandas as pd
import numpy as np
from sqlalchemy_proj8 import validate_artwork_data
from sqlalchemy_proj8 import create_database_engine


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
    assert all(result_df.dtypes == [object, object, object, object, np.int64, np.int64])

    with open('errors.log', 'r') as log_file:
        log_contents = log_file.read()
        assert log_contents.strip() == ""


def test_validate_artwork_data():
    input_data = pd.DataFrame({'Artist': ['Otto Wagner', 'Christian de Portzamparc'],
                               'ConstituentID': ["6210", '7470'],
                               'ArtistBio': ['Austrian, 1841–1918', 'French, born 1944'],
                               'Nationality': ['Austrian', 'French'],
                               'Gender': ['Male', 'Male'],
                               'Medium': ['Ink and cut-and-pasted painted pages on paper', 'Paint and colored pencil on print'],
                               'Dimensions': ['19 1/8 x 66 1/2" 48.6 x 168.9 cm', '16 x 11 3/4" 40.6 x 29.8 cm'],
                               'CreditLine': ['Fractional and promised gift of Jo Carole and Ronald S. Lauder', 'Gift of the architect in honor of Lily Auchincloss'],
                               'AccessionNumber': ['885.1996', '1.1995'],
                               'Classification': ['Architecture', 'Architecture'],
                               'Department': ['Architecture & Design', 'Architecture & Design'],
                               'DateAcquired': ['1996-04-09', '1995-01-17'],
                               'Cataloged': ['Y', 'Y'],
                               'ObjectID': ['2', '3']})

    expected_output = pd.DataFrame({'Artist': ['Otto Wagner', 'Christian de Portzamparc'],
                                    'ConstituentID': ['6210', '7470'],
                                    'ArtistBio': ['Austrian, 1841–1918', 'French, born 1944'],
                                    'Nationality': ['Austrian', 'French'],
                                    'Gender': ['Male', 'Male'],
                                    'Medium': ['Ink and cut-and-pasted painted pages on paper', 'Paint and colored pencil on print'],
                                    'Dimensions': ['19 1/8 x 66 1/2" 48.6 x 168.9 cm', '16 x 11 3/4" 40.6 x 29.8 cm'],
                                    'CreditLine': ['Fractional and promised gift of Jo Carole and Ronald S. Lauder', 'Gift of the architect in honor of Lily Auchincloss'],
                                    'AccessionNumber': ['885.1996', '1.1995'],
                                    'Classification': ['Architecture', 'Architecture'],
                                    'Department': ['Architecture & Design', 'Architecture & Design'],
                                    'DateAcquired': ['1996-04-09', '1995-01-17'],
                                    'Cataloged': ['Y', 'Y'],
                                    'ObjectID': ['2', '3'],})

    result = validate_artwork_data(input_data)

    assert result.shape == expected_output.shape and result.sort_values(by=result.columns.tolist()).equals(
        expected_output.sort_values(by=expected_output.columns.tolist()))

    assert len(result) == len(expected_output)
    assert result.columns.tolist() == expected_output.columns.tolist()


def test_create_database_engine_invalid_input():
    postgres_key = [None,"invalid_key"]
    result = create_database_engine(postgres_key)
    assert result is None

