from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import DATABASE_URL

engine = create_engine(DATABASE_URL)
metadata = MetaData()
Base = declarative_base()

Session = sessionmaker(bind=engine)
session = Session()
