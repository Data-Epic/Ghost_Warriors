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
        "BeginDate": [1000, "Avengers", 2000],
        "EndDate": [2020, 2022, 2010]
    }
    df2 = pd.DataFrame(data2, columns=columns)

    return df1, df2

@pytest.fixture(autouse=True)
def create_df_artwork():
    input_data = pd.DataFrame({'Artist': ['Otto Wagner', 'Christian de Portzamparc'],
                               'ConstituentID': ["6210", '7470'],
                               'ArtistBio': ['Austrian, 1841–1918', 'French, born 1944'],
                               'Nationality': ['Austrian', 'French'],
                               'Gender': ['Male', 'Male'],
                               'Medium': ['Ink and cut-and-pasted painted pages on paper',
                                          'Paint and colored pencil on print'],
                               'Dimensions': ['19 1/8 x 66 1/2" 48.6 x 168.9 cm', '16 x 11 3/4" 40.6 x 29.8 cm'],
                               'CreditLine': ['Fractional and promised gift of Jo Carole and Ronald S. Lauder',
                                              'Gift of the architect in honor of Lily Auchincloss'],
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
                                    'Medium': ['Ink and cut-and-pasted painted pages on paper',
                                               'Paint and colored pencil on print'],
                                    'Dimensions': ['19 1/8 x 66 1/2" 48.6 x 168.9 cm', '16 x 11 3/4" 40.6 x 29.8 cm'],
                                    'CreditLine': ['Fractional and promised gift of Jo Carole and Ronald S. Lauder',
                                                   'Gift of the architect in honor of Lily Auchincloss'],
                                    'AccessionNumber': ['885.1996', '1.1995'],
                                    'Classification': ['Architecture', 'Architecture'],
                                    'Department': ['Architecture & Design', 'Architecture & Design'],
                                    'DateAcquired': ['1996-04-09', '1995-01-17'],
                                    'Cataloged': ['Y', 'Y'],
                                    'ObjectID': ['2', '3'], })
    return input_data, expected_output
