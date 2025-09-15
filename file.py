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
    print("Profiling Report for:", file_path)
    print('Total rows are:\n', total_rows)
    print('Total columns are:\n', total_cols)
<<<<<<< Updated upstream
=======
    print('Total Duplicate count:\n', duplicate_count)
    print('Duplicate Percentage:\n', duplicate_percentage)
>>>>>>> Stashed changes
    for col in df.columns:
        col_dup_count = df[col].duplicated().sum()   
        col_dup_percent = round((col_dup_count / total_rows) * 100, 2)

        print(f"Column: {col}")
        print(f"  Null Count: {null_counts[col]}")
        print(f"  Null %: {null_percentage[col]}%")
        print(f"  Duplicate Count: {col_dup_count}")
        print(f"  Duplicate %: {col_dup_percent}%")


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
      
