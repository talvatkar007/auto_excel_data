from sqlalchemy import Column, Integer, String, Float
from db_setup import Base

class Employee(Base):
    __tablename__ = 'employees'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    Name = Column(String)
    Department = Column(String)
    Salary = Column(Float)
