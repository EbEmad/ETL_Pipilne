import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from db_connection import connect_to_db  # Assume this contains your DB connection logic
from config import DATA_FOLDER  # Adjust this to your actual data folder

# Extract step: Read the CSV file
def extract_data(file_path):
    df = pd.read_csv(file_path)
    print(f"Data extracted successfully from {file_path}.")
    return df

# Transform step
def transform_data(df):
    # Ensure the necessary columns are present
    required_columns = ['Pregnancies', 'Glucose', 'BloodPressure', 
                        'SkinThickness', 'Insulin', 'BMI', 
                        'DiabetesPedigreeFunction', 'Age', 'Outcome']
    
    # Check for missing columns
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns in the data: {missing_columns}")

    # You might want to handle data types, check for null values, etc.
    # Here, you can implement any additional transformations as needed.
    
    return df  # Return the transformed DataFrame

# Load step
def load_data_to_sql(df, engine):
    try:
        df.to_sql('DiabetesRecords', con=engine, if_exists='append', index=False)
        print("Data loaded successfully.")
    except SQLAlchemyError as e:
        print(f"Error occurred while inserting data: {e}")

# Main ETL function
def etl_pipeline():
    engine = connect_to_db()

    for file_name in os.listdir(DATA_FOLDER):
        if file_name.endswith(".csv"):  # Check for CSV files
            file_path = os.path.join(DATA_FOLDER, file_name)
            
            # Extract
            df = extract_data(file_path)
            
            # Transform
            df_transformed = transform_data(df)
            
            # Load
            load_data_to_sql(df_transformed, engine)

# Run the ETL pipeline
if __name__ == "__main__":
    etl_pipeline()
