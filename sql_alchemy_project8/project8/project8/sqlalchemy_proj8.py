from sqlalchemy.orm import Session, Mapped, declarative_base, relationship, mapped_column
from sqlalchemy import create_engine, insert, Index, select, Integer, Column, String, PrimaryKeyConstraint, ForeignKey
from typing import Optional
from sqlalchemy.exc import IntegrityError
import pandas as pd
from dotenv import load_dotenv
import os
import logging

load_dotenv()

PATH_ERROR = os.getenv("PATH_ERROR")
POSTGRES_KEY = os.getenv("POSTGRES_KEY")
PATH_DATA = os.getenv("PATH_DATA")
ARTWORK_CSV = os.getenv('ARTWORK_CSV')

logging.basicConfig(filename=PATH_ERROR, level=logging.ERROR,
                    format='%(levelname)s (%(asctime)s) : %(message)s (%(lineno)d)')

# columns to be loaded in from data
columns = ["DisplayName", "ArtistBio", "Nationality", "Gender", "BeginDate", "EndDate"]
artist_df = pd.read_csv('Artists.csv', usecols=[col for col in columns])
columns2 = ['Artist', 'ConstituentID', 'ArtistBio', 'Nationality',
            'Gender', 'Medium', 'Dimensions', 'CreditLine',
            'AccessionNumber', 'Classification', 'Department',
            'Cataloged', 'ObjectID']
artwork_df = pd.read_csv(ARTWORK_CSV, usecols=[col for col in columns2],low_memory=False)


# connecting to database
def create_database_engine(postgres_key):
    '''
    this function creates a database engine
    '''
    try:
        database_engine = create_engine(postgres_key)
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

    id: Mapped[int] = mapped_column(primary_key=True)
    DisplayName: Mapped[str] = mapped_column(primary_key=True)
    ArtistBio: Mapped[str]
    Nationality: Mapped[Optional[str]]
    Gender: Mapped[Optional[str]]
    BeginDate = mapped_column(Integer)
    EndDate = mapped_column(Integer)

    __table_args__ = (
        Index('my_index', "Nationality", "Gender", postgresql_using='btree'),
    )

    def __repr__(self):
        return f"DisplayName({self.DisplayName}), ArtistBio({self.ArtistBio}), Nationality({self.Nationality}), Gender({self.Gender})"


class Artwork(Base):
    __tablename__ = "Artwork"

    Artist = Column(String)
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
    """function checks that each
    element in each row is of expected
    datatype """

    # creating copy of data
    data = data_.copy()

    str_columns = ["DisplayName", "ArtistBio", "Nationality", "Gender"]
    # loop through data and check if each is of expected data type
    for num in range(len(data)):
        validation_condition = all(isinstance(data.loc[num, col], str) for col in str_columns) and all(data.loc[num, :].notna())
        if validation_condition:
            try:
                #attempt to cast columns to int datatype
                data.loc[num, ['BeginDate', 'EndDate']] = data.loc[num, ['BeginDate', 'EndDate']].astype(int)
            except:
                data = data.drop(index=num)
                #logging error message
                logging.error(f"Invalid Data Type in row")
        else:
            data = data.drop(index=num)
            logging.error(f"Invalid Data Type in row")
    data[['BeginDate','EndDate']] = data[['BeginDate','EndDate']].astype(int)
    return data.reset_index(drop=True)


def validate_artwork_data(data_):
    """Function checks that each element in each row is of expected datatype."""
    data = data_.copy()

    for num in range(len(data)):
        try:
            if data.loc[num, :].notna().all():
                if (
                        isinstance(data.loc[num, "Artist"], str) and
                        isinstance(data.loc[num, "ConstituentID"], (str, int)) and
                        isinstance(data.loc[num, "ArtistBio"], str) and
                        isinstance(data.loc[num, "Nationality"], str) and
                        isinstance(data.loc[num, "Gender"], str) and
                        isinstance(data.loc[num, "Medium"], str) and
                        isinstance(data.loc[num, "CreditLine"], str) and
                        isinstance(data.loc[num, "AccessionNumber"], str) and
                        isinstance(data.loc[num, "Classification"], str) and
                        isinstance(data.loc[num, "Department"], str) and
                        isinstance(data.loc[num, "ObjectID"], str)
                ):
                    data.loc[num, ['Dimensions', 'Cataloged']] = data.loc[num, ['Dimensions', 'Cataloged']].astype(str)
                else:
                    data = data.drop(index=num)
                    logging.error(f"Invalid Data Type in row {num}")
            else:
                data = data.drop(index=num)
                logging.error(f"NaN values in row {num}")
        except Exception as e:
            data = data.drop(index=num)
            logging.error(f"Error processing row {num}: {str(e)}")

    return data.reset_index(drop=True)

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
                session.rollback()  # Rollback the session to avoid partial commits
                print(f"Skipping row with DisplayName '{data.DisplayName}' due to duplicate key.")


def update_database_with_artwork():
    artwork = artwork_df

    with Session(engine) as session:
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
    logging.info("")
    with Session(engine) as session:
        # get 5 rows of data
        sample_query = select(Artist).limit(5)
        result_sample = session.execute(sample_query).fetchall()
        logging.info("")
        logging.info(f"Sample of records: \n {result_sample}")

        # get 5 rows of data where nationality is American
        filter_query = select(Artist).where(Artist.Nationality == "American").limit(5)
        result_filtered = session.execute(filter_query).fetchall()
        logging.info("")
        logging.info(f"Filtered records: \n {result_filtered}")

        # order data by begin date and get 5 rows
        order_query = select(Artist).order_by(Artist.BeginDate).limit(5)
        result_ordered = session.execute(order_query).fetchall()
        logging.info("")
        logging.info(f"Ordered records: \n {result_ordered}")

        # get 5 rows of male artists
        order_query = select(Artist).where(Artist.Gender == 'Male').limit(5)
        result_ordered = session.execute(order_query).fetchall()
        logging.info("")
        logging.info(f"Ordered records: \n {result_ordered}")

        # get 5 rows of female artists
        order_query = select(Artist).where(Artist.Gender == 'Female').limit(5)
        result_ordered = session.execute(order_query).fetchall()
        logging.info("")
        logging.info(f"Ordered records: \n {result_ordered}")


if __name__ == "__main__":
    update_database()
    update_database_with_artwork()
    query_database()
