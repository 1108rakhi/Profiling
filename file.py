import pandas as pd
import argparse

def profiling(file_path):
    df = pd.read_csv(file_path)
    total_rows = len(df)
    total_cols = df.shape[1]
    null_counts = df.isnull().sum()
    null_percentage = round((null_counts/total_rows)*100 , 2)
    duplicate_count = df.duplicated().sum()
    duplicate_percentage = round((duplicate_count / total_rows) * 100, 2)
    print('Total rows are:\n', total_rows)
    print('Total columns are:\n', total_cols)
    print('Count of null values:\n', null_counts)
    print('Percentage of null values:\n', null_percentage)
    print('Number of duplicates:\n', duplicate_count)
    print('Perccentage of duplicates:\n', duplicate_percentage)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI-based CSV profiling")
    parser.add_argument(
    "file",
    nargs="?",  
    default=r"C:\Users\Rightdatauser\Documents\CLI_Based_Profiling\profiling_demo.csv",
    help="Path to the CSV file"
)

    args = parser.parse_args()

    profiling(args.file)
      
