import pandas as pd
from sqlalchemy import Table, Column, Integer, Float, Text
from db_setup import engine, metadata
from config import EXCEL_FILE

def get_sqlalchemy_type(series):
    if pd.api.types.is_integer_dtype(series):
        return Integer
    elif pd.api.types.is_float_dtype(series):
        return Float
    else:
        return Text

def process_excel():
    excel_data = pd.read_excel(EXCEL_FILE, sheet_name=None)
    print("Excel file read successfully. Processing sheets...")

    with engine.connect() as connection:
        for sheet_name, df in excel_data.items():
            print(f"\nProcessing sheet: {sheet_name}")

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

            existing_ids_query = f'SELECT "{df.columns[0]}" FROM {table_name}'
            existing_ids = pd.read_sql(existing_ids_query, engine)[df.columns[0]].tolist()

            new_data = df[~df[df.columns[0]].isin(existing_ids)]

            if new_data.empty:
                print(f"No new data to insert for '{table_name}'. Skipping...")
            else:
                new_data.to_sql(table_name, engine, if_exists='append', index=False)
                print(f"Inserted {len(new_data)} new rows into '{table_name}'.")

    print("\nAll sheets processed successfully!")
