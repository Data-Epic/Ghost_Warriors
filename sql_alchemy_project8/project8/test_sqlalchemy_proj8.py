from sqlalchemy_proj8 import validate_artist_df
from sqlalchemy_proj8 import validate_artwork_data
from sqlalchemy_proj8 import create_database_engine



def test_validate_artist_df(create_df_artist):
    df1,df2 = create_df_artist
    result_df = validate_artist_df(df1)

    assert len(result_df) == 3
    assert all(result_df.dtypes == [object, object, object, object, int,int])

    # with open('error.log', 'r') as log_file:
    #     log_contents = log_file.read()
    #     assert log_contents.strip() == ""

    result_df = validate_artist_df(df2)
    assert len(result_df) == 1
    assert all(result_df.dtypes == [object, object, object, object, int,int])

    # with open('error.log', 'r') as log_file:
    #     log_contents = log_file.read()
    #     assert log_contents.strip() != ""
#

def test_validate_artwork_data(create_df_artwork):
    input_data, expected_output = create_df_artwork

    result = validate_artwork_data(input_data)

    assert result.shape == expected_output.shape and result.sort_values(by=result.columns.tolist()).equals(
        expected_output.sort_values(by=expected_output.columns.tolist()))

    assert len(result) == len(expected_output)
    assert result.columns.tolist() == expected_output.columns.tolist()


def test_create_database_engine_invalid_input():
    postgres_key = [None,"invalid_key"]
    result = create_database_engine(postgres_key)
    assert result is None

