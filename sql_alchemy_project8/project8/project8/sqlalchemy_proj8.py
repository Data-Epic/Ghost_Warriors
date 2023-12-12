from sqlalchemy.orm import Session, mapped_column, Mapped
from sqlalchemy import create_engine, insert, Index, select, Integer
from sqlalchemy.orm import declarative_base
from typing import Optional
import pandas as pd
from dotenv import load_dotenv
import os
import logging

load_dotenv()

PATH_ERROR= os.getenv("PATH_ERROR")
POSTGRES_KEY = os.getenv("POSTGRES_KEY")
PATH_DATA = os.getenv("PATH_DATA")

logging.basicConfig(filename='errors.log', level=logging.ERROR,
                    format = '%(levelname)s (%(asctime)s) : %(message)s (%(lineno)d)')

#columns too be loaded in from data
columns = ["DisplayName","ArtistBio","Nationality","Gender","BeginDate","EndDate"]
artist_df = pd.read_csv('Artists.csv',usecols=[col for col in columns])
#connecting to database
engine = create_engine(POSTGRES_KEY)

Base = declarative_base()

#creating table in database
class Artist(Base):
    __tablename__ = "Artist"

    id: Mapped[int] = mapped_column(primary_key=True)
    DisplayName: Mapped[str] = mapped_column(primary_key=True)
    ArtistBio: Mapped[str]
    Nationality: Mapped[Optional[str]]
    Gender: Mapped[Optional[str]]
    BeginDate= mapped_column(Integer)
    EndDate= mapped_column(Integer)
    
    __table_args__ = (
        Index('my_index', "Nationality", "Gender",postgresql_using='btree'),
    )

    def __repr__(self):
        return f"DisplayName({self.DisplayName}), ArtistBio({self.ArtistBio}), Nationality({self.Nationality}), Gender({self.Gender})"

Base.metadata.create_all(engine)


def validate_artist_df(data_):
        """function checks that each
        element in each row is of expected 
        datatype """

        #creating copy of data
        data = data_.copy()
        #loop through data and check if each is of expected data type
        for num in range(len(data)):
            if (isinstance(data.loc[num,"DisplayName"], str) and
                isinstance(data.loc[num,"ArtistBio"], str) and
                isinstance(data.loc[num,"Nationality"], str) and
                isinstance(data.loc[num,"Gender"], str) and
                all(data.loc[num,:].notna())):
                
                try:
                    #attempt to cast data in columns to int
                    data.loc[num,['BeginDate','EndDate']].astype(int)
                except:
                    #drop row 
                    data = data.drop(index=num)
                    #log error message to error.log file
                    logging.error(f"Invalid Data Type in row")
                    continue
            else:
                #drop row
                data = data.drop(index=num)
                #log error message to error.log file
                logging.error(f"Invalid Data Type in row")

        return data.reset_index(drop=True)


def update_database():
    """Updates Database table Artist
    with validated data from Dataframe"""
    #validate dataframe
    artist = validate_artist_df(artist_df)

    with Session(engine) as session:
        #truncate table Artist
        session.query(Artist).delete()
        session.commit()
        #loading each row to database table Artist
        for num in artist.T:
            data = artist.loc[num,:]
            row = Artist(
            id = num,
            DisplayName=data.DisplayName,
            ArtistBio=data.ArtistBio,
            Nationality = data.Nationality,
            Gender = data.Gender,
            BeginDate=int(data.BeginDate),
            EndDate=int(data.EndDate))
            session.add(row)
            session.commit()

def query_database():
    """Queries Database to get 
    insight from Data"""
    print("Queries for DataBase")
    print()
    with Session(engine) as session:
        #get 5 rows of data
        sample_query = select(Artist).limit(5)
        result_sample = session.execute(sample_query).fetchall()
        print()
        print(f"Sample of records: \n {result_sample}")

        #get 5 rows of data where nationality is American
        filter_query = select(Artist).where(Artist.Nationality=="American").limit(5)
        result_filtered = session.execute(filter_query).fetchall()
        print()
        print(f"Filtered records: \n {result_filtered}")
        
        #order data by begin date and get 5 rows
        order_query = select(Artist).order_by(Artist.BeginDate).limit(5)
        result_ordered = session.execute(order_query).fetchall()
        print()
        print(f"Ordered records: \n {result_ordered}")

        #get 5 rows of male artists
        order_query = select(Artist).where(Artist.Gender =='Male').limit(5)
        result_ordered = session.execute(order_query).fetchall()
        print()
        print(f"Ordered records: \n {result_ordered}")

        #get 5 rows of female artists
        order_query = select(Artist).where(Artist.Gender =='Female').limit(5)
        result_ordered = session.execute(order_query).fetchall()
        print()
        print(f"Ordered records: \n {result_ordered}")



if __name__ == "__main__":
    update_database()
    query_database()