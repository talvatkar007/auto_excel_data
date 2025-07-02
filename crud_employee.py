from db_setup import session
from tables import Employee

def create_employee(id, name, department, salary):
    new_employee = Employee(ID=id, Name=name, Department=department, Salary=salary)
    session.add(new_employee)
    session.commit()
    print(f"Employee '{name}' created successfully with ID {new_employee.ID}")

def read_employees():
    employees = session.query(Employee).all()
    for emp in employees:
        print(f"ID: {emp.ID}, Name: {emp.Name}, Department: {emp.Department}, Salary: {emp.Salary}")

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
