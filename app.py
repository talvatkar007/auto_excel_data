from db_setup import Base, engine, session
from tables import Employee
import excel_to_postgres as exl
import crud_employee as crud

# ---------------- Main Execution ----------------
if __name__ == "__main__":
    Base.metadata.create_all(engine)
    
    # Process Excel and load data
    exl.process_excel()
    
    # CRUD Example Operations
    # crud.create_employee(11, "Ajay", "IT", 75000)
    # crud.read_employees()
    # crud.get_employee_by_id(11)
    # crud.update_employee(11, department="Finance", salary=85000)
    # crud.delete_employee(11)

    session.close() 
    print("All operations completed.")
