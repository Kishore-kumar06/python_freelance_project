import pandas as pd

# This function reads and clean pipeline names from csv file to be used for selenium operations. It removes any leading or trailing whitespace from the pipeline names and drops any rows with missing values.
def read_and_clean_csv(file_path):
    df = pd.read_csv(file_path)
    if df is not None:
        # Example cleaning steps (these can be modified based on actual data)
        df.dropna(inplace=True)  # Remove rows with missing values
        df['PipelineName'] = df['PipelineName'].str.strip()  # Remove leading/trailing whitespace
        return df


