import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, Text, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import psycopg2

# ---------------- Configuration ----------------
DATABASE_URL = 'postgresql+psycopg2://postgres:123456@localhost:5432/exl_data'
excel_file = r'D:\SPC\Python\Assignments\Automate PostgreSQL To Excel\sample_datasets.xlsx'
# ------------------------------------------------

# Database Setup
engine = create_engine(DATABASE_URL)
metadata = MetaData()
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

# ---------------- Helper to Map Data Types ----------------
def get_sqlalchemy_type(series):
    if pd.api.types.is_integer_dtype(series):
        return Integer
    elif pd.api.types.is_float_dtype(series):
        return Float
    else:
        return Text

# ---------------- Read & Insert Excel Data ----------------
excel_data = pd.read_excel(excel_file, sheet_name=None)
print("Excel file read successfully. Processing sheets...")

# read the excel file
# employees = pd.read_excel("D:\SPC\Python\Assignments\Automate PostgreSQL To Excel\sample_datasets.xlsx", sheet_name="Employees")

# products = pd.read_excel("D:\SPC\Python\Assignments\Automate PostgreSQL To Excel\sample_datasets.xlsx", sheet_name="Products")

# Sales = pd.read_excel("D:\SPC\Python\Assignments\Automate PostgreSQL To Excel\sample_datasets.xlsx", sheet_name="Sales")

# Customers = pd.read_excel("D:\SPC\Python\Assignments\Automate PostgreSQL To Excel\sample_datasets.xlsx", sheet_name="Customers")

# PRINT THE DATA
# print(employees.head())

# print(products.head)

# print(Sales.head())        

# print(Customers.head())
with engine.connect() as connection:
    for sheet_name, df in excel_data.items():
        print(f"\n Processing sheet: {sheet_name}")

        table_name = sheet_name.lower()
        columns = []

        for idx, col in enumerate(df.columns):
            col_type = get_sqlalchemy_type(df[col])
            if idx == 0:
                columns.append(Column(col, col_type, primary_key=True))
            else:
                columns.append(Column(col, col_type))

        table = Table(table_name, metadata, *columns)
        metadata.create_all(engine, tables=[table])
        print(f"Table '{table_name}' ready.")

        # Check for existing primary keys to avoid duplicates
        existing_ids_query = f'SELECT "{df.columns[0]}" FROM {table_name}'
        existing_ids = pd.read_sql(existing_ids_query, engine)[df.columns[0]].tolist()

        new_data = df[~df[df.columns[0]].isin(existing_ids)]

        if new_data.empty:
            print(f"No new data to insert for '{table_name}'. Skipping...")
        else:
            new_data.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Inserted {len(new_data)} new rows into '{table_name}'.")

print("\nAll sheets processed successfully!")

# ---------------- Employee ORM Class ----------------
class Employee(Base):
    __tablename__ = 'employees'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    Name = Column(String)
    Department = Column(String)
    Salary = Column(Float)

Base.metadata.create_all(engine)

# ---------------- Employee CRUD Operations ----------------

def create_employee(id,name, department, salary):
    new_employee = Employee(ID=id, Name=name, Department=department, Salary=salary)
    session.add(new_employee)
    session.commit()
    print(f"Employee '{name}' created successfully with ID {new_employee.ID}")

def read_employees():
    employees = session.query(Employee).all()
    for emp in employees:
        print(f"ID: {emp.ID}, Name: {emp.Name}, Department: {emp.Department}, Salary: {emp.Salary}")
    print("All employees read successfully.")

def get_employee_by_id(emp_id):
    emp = session.query(Employee).filter(Employee.ID == emp_id).first()
    if emp:
        print(f"ID: {emp.ID}, Name: {emp.Name}, Department: {emp.Department}, Salary: {emp.Salary}")
    else:
        print(f"Employee with ID {emp_id} not found.")

def update_employee(emp_id, name=None, department=None, salary=None):
    emp = session.query(Employee).filter(Employee.ID == emp_id).first()
    if emp:
        if name:
            emp.Name = name
        if department:
            emp.Department = department
        if salary:
            emp.Salary = salary
        session.commit()
        print(f"Employee {emp_id} updated successfully.")
    else:
        print(f"Employee with ID {emp_id} not found.")

def delete_employee(emp_id):
    emp = session.query(Employee).filter(Employee.ID == emp_id).first()
    if emp:
        session.delete(emp)
        session.commit()
        print(f"Employee {emp_id} deleted successfully.")
    else:
        print(f"Employee with ID {emp_id} not found.")



# ---------------- CRUD Operations ----------------

# create_employee(11,"Ajay", "IT", 75000)
# read_employees()
# get_employee_by_id(1)
# update_employee(1, department="Finance", salary=80000)
# delete_employee(1)

session.close()

