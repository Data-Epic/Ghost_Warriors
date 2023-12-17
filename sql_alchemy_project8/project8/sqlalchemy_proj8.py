from sqlalchemy.orm import Session, declarative_base
from sqlalchemy import create_engine, Index, select, Integer, Column, String, PrimaryKeyConstraint
from sqlalchemy.exc import IntegrityError
import pandas as pd
from dotenv import load_dotenv
import os
import logging

load_dotenv()

PATH_ERROR = os.getenv("PATH_ERROR")
POSTGRES_KEY = os.getenv("POSTGRES_KEY")
PATH_DATA = os.getenv("PATH_DATA")
ARTWORKS_CSV = os.getenv('ARTWORK_CSV')
ARTISTS_CSV = os.getenv('ARTIST_CSV')

logging.basicConfig(filename=PATH_ERROR, level=logging.ERROR,
                    format='%(levelname)s (%(asctime)s) : %(message)s (%(lineno)d)')

# columns to be loaded in from data
artist_columns = ["DisplayName", "ArtistBio", "Nationality", "Gender", "BeginDate", "EndDate"]
artist_df = pd.read_csv(ARTISTS_CSV, usecols=[col for col in artist_columns])

artwork_columns = ['Artist', 'ConstituentID', 'ArtistBio', 'Nationality',
                   'Gender', 'Medium', 'Dimensions', 'CreditLine',
                   'AccessionNumber', 'Classification', 'Department',
                   'Cataloged', 'ObjectID']
artwork_df = pd.read_csv(ARTWORKS_CSV, usecols=[col for col in artwork_columns])


# connecting to database
def create_database_engine(postgres_key):
    '''
    This function creates a database engine
    '''
    try:
        database_engine = create_engine(postgres_key, echo=True)
        logging.info("Database engine created successfully.")
        return database_engine
    except Exception as e:
        logging.info(f"Error creating database engine: {e}")
        return None


engine = create_database_engine(POSTGRES_KEY)

Base = declarative_base()


# creating table in database

class Artist(Base):
    __tablename__ = "artist"

    id = Column(Integer, primary_key=True)
    DisplayName = Column(String, primary_key=True, unique=True)
    ArtistBio = Column(String)
    Nationality = Column(String)
    Gender = Column(String)
    BeginDate = Column(Integer)
    EndDate = Column(Integer)

    __table_args__ = (
        Index('my_index', "Nationality", "Gender", postgresql_using='btree'),
    )

    def __repr__(self):
        return f"DisplayName({self.DisplayName}), ArtistBio({self.ArtistBio}), Nationality({self.Nationality}), Gender({self.Gender})"


class Artwork(Base):
    __tablename__ = "artwork"

    Artist = Column(String)  # , ForeignKey('artist.DisplayName')
    ConstituentID = Column(String)
    ArtistBio = Column(String)
    Nationality = Column(String)
    Gender = Column(String)
    Medium = Column(String)
    Dimensions = Column(String)
    CreditLine = Column(String)
    AccessionNumber = Column(String)
    Classification = Column(String)
    Department = Column(String)
    Cataloged = Column(String)
    ObjectID = Column(String)

    __table_args__ = (
        PrimaryKeyConstraint('AccessionNumber', 'ObjectID'),
        Index("idx_accessionnumber_objectid", 'Department', 'Nationality', postgresql_using='btree')
    )


Base.metadata.create_all(engine)


def validate_artist_df(data_):
    """This function checks that each
    element in each row is of expected
    datatype """

    # creating copy of data
    data = data_.copy()

    str_columns = ["DisplayName", "ArtistBio", "Nationality", "Gender"]
    # loop through data and check if each is of expected data type
    for num in range(len(data)):
        validation_condition = all(isinstance(data.loc[num, col], str) for col in str_columns) and all(
            data.loc[num, :].notna())
        if validation_condition:
            try:
                # attempt to cast columns to int datatype
                data.loc[num, ['BeginDate', 'EndDate']] = data.loc[num, ['BeginDate', 'EndDate']].astype(int)
            except:
                data = data.drop(index=num)
                # logging error message
                logging.debug(f"Invalid Data Type in row")
        else:
            data = data.drop(index=num)
            logging.error(f"Invalid Data Type in row")
    data[['BeginDate', 'EndDate']] = data[['BeginDate', 'EndDate']].astype(int)
    return data.reset_index(drop=True)


def validate_artwork_data(data_):
    """Function checks that each element in each row is of expected datatype."""
    data = data_.copy()
    invalid_rows = []

    for num in range(len(data)):
        try:
            # Check if all values in the row are not NaN
            if not data.loc[num, :].notna().all():
                invalid_rows.append(num)
                logging.debug(f"NaN values in row {num}")
                continue

            data_types = {
                'Artist': str,
                'ConstituentID': (str, int),  # Allowing either str or int
                'ArtistBio': str,
                'Nationality': str,
                'Gender': str,
                'Medium': str,
                'CreditLine': str,
                'AccessionNumber': str,
                'Classification': str,
                'Department': str,
                'ObjectID': str
            }

            # Check if all values are of the correct type
            for col, data_type in data_types.items():
                if not isinstance(data.at[num, col], data_type if not isinstance(data_type, tuple) else data_type[0]):
                    if isinstance(data_type, tuple) and not isinstance(data.at[num, col], data_type[1]):
                        invalid_rows.append(num)
                        logging.debug(f"Invalid Data Type in row {num} for column {col}")
                        break
            else:
                # If the types are correct, ensure 'Dimensions' and 'Cataloged' are strings
                data.loc[num, ['Dimensions', 'Cataloged']] = data.loc[num, ['Dimensions', 'Cataloged']].astype(str)

        except Exception as e:
            invalid_rows.append(num)
            logging.error(f"Error processing row {num}: {str(e)}")

    # Drop all invalid rows at once
    data = data.drop(index=invalid_rows).reset_index(drop=True)

    return data


def update_database():
    """Updates Database table Artist
    with validated data from Dataframe"""
    artist = validate_artist_df(artist_df)

    with Session(engine) as session:
        session.query(Artist).delete()
        session.commit()

        # loading each row to database table Artist
        for num in artist.index:
            data = artist.loc[num, :]

            try:
                row = Artist(
                    id=num,
                    DisplayName=data.DisplayName,
                    ArtistBio=data.ArtistBio,
                    Nationality=data.Nationality,
                    Gender=data.Gender,
                    BeginDate=int(data.BeginDate),
                    EndDate=int(data.EndDate))

                session.add(row)
                session.commit()

            except IntegrityError as e:
                session.rollback()
                print(f"Skipping row with DisplayName '{data.DisplayName}' due to duplicate key.")


def update_database_with_artwork():
    """Updates Database table artwork
       with validated data from Dataframe"""
    artwork = validate_artwork_data(artwork_df)

    with Session(engine) as session:
        session.query(Artwork).delete()
        session.commit()
        for num, data in artwork.iterrows():
            if pd.isna(data['AccessionNumber']) or pd.isna(data['ObjectID']):
                continue

            row = Artwork(
                Artist=data['Artist'],
                ConstituentID=str(data['ConstituentID']),
                ArtistBio=data['ArtistBio'],
                Nationality=data['Nationality'],
                Gender=data['Gender'],
                Medium=data['Medium'],
                Dimensions=data['Dimensions'],
                CreditLine=data['CreditLine'],
                AccessionNumber=data['AccessionNumber'],
                Classification=data['Classification'],
                Department=data['Department'],
                Cataloged=data['Cataloged'],
                ObjectID=data['ObjectID']
            )
            session.add(row)
            session.commit()


def query_database():
    """Queries Database to get 
    insight from Data"""
    logging.info("Queries for DataBase")
    logging.info('')
    with Session(engine) as session:
        # get 5 rows of data
        sample_query = select(Artist).limit(5)
        result_sample = session.execute(sample_query).fetchall()
        logging.info('')
        logging.info(f"Sample of records: \n {result_sample}")

        # get 5 rows of data where nationality is American
        filter_query = select(Artist).where(Artist.Nationality == "American").limit(5)
        result_filtered = session.execute(filter_query).fetchall()
        logging.info('')
        logging.info(f"Filtered records: \n {result_filtered}")

        # order data by begin date and get 5 rows
        order_query = select(Artist).order_by(Artist.BeginDate).limit(5)
        result_ordered = session.execute(order_query).fetchall()
        logging.info('')
        logging.info(f"Ordered records: \n {result_ordered}")

        # get 5 rows of male artists
        order_query = select(Artist).where(Artist.Gender == 'Male').limit(5)
        result_ordered = session.execute(order_query).fetchall()
        logging.info('')
        logging.info(f"Ordered records: \n {result_ordered}")

        # get 5 rows of female artists
        order_query = select(Artist).where(Artist.Gender == 'Female').limit(5)
        result_ordered = session.execute(order_query).fetchall()
        logging.info('')
        logging.info(f"Ordered records: \n {result_ordered}")


if __name__ == "__main__":
    update_database()
    update_database_with_artwork()
    query_database()
