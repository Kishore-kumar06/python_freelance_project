import pandas as pd

# file_path = r"D:\Project\python_freelance_project\data\source_files\pipelinenames.csv"

def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return None
    
def clean_data(file_path):
    df = read_csv_file(file_path)
    if df is not None:
        # Example cleaning steps (these can be modified based on actual data)
        df.dropna(inplace=True)  # Remove rows with missing values
        df['PipelineName'] = df['PipelineName'].str.strip()  # Remove leading/trailing whitespace
        return df


