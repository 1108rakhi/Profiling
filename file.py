import pandas as pd
import argparse

def profiling(file_path, save_csv = False):
    # version 1 
    df = pd.read_csv(file_path)
    total_rows = len(df)
    total_cols = df.shape[1]
    null_counts = df.isnull().sum()
    null_percentage = round((null_counts/total_rows)*100 , 2)
    duplicate_count = df.duplicated().sum()
    duplicate_percentage = round((duplicate_count / total_rows) * 100, 2)
    print('Total rows are:\n', total_rows)
    print('Total columns are:\n', total_cols)
    for col in df.columns:
        col_dup_count = df[col].duplicated().sum()   
        col_dup_percent = round((col_dup_count / total_rows) * 100, 2)

        print(f"Column: {col}")
        print(f"  Null Count: {null_counts[col]}")
        print(f"  Null %: {null_percentage[col]}%")
        print(f"  Duplicate Count: {col_dup_count}")
        print(f"  Duplicate %: {col_dup_percent}%")

# version 2 --> csv based output
    if save_csv:
        metrics = []
        metric_types = ["Total Rows","Total Columns","Row-wise Duplicate Count","Row-wise Duplicate %"]
        values = [total_rows, total_cols, duplicate_count, duplicate_percentage]
        metrics.extend({"Metric Type": m, "Value": v} for m, v in zip(metric_types, values))
        for col in df.columns:
                col_dup_count = df[col].duplicated().sum()
                col_dup_percent = round((col_dup_count / total_rows) * 100, 2)
                metric_types = [
                    f"Null Count [{col}]",
                    f"Null % [{col}]",
                    f"Duplicate Count [{col}]",
                    f"Duplicate % [{col}]"
                ]
                values = [null_counts[col], null_percentage[col], col_dup_count, col_dup_percent]
                metrics.extend({"Metric Type": m, "Value": v} for m, v in zip(metric_types, values))

        report = pd.DataFrame(metrics)
        output_file = file_path.replace(".csv", "_profiling_report.csv")
        report.to_csv(output_file, index=False)
        print('csv saved')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI-based CSV profiling")
    parser.add_argument(
    "file",
    nargs="?",  
    default=r"C:\Users\Rightdatauser\Documents\CLI_Based_Profiling\profiling_demo.csv",
    help="Path to the CSV file"
)
    parser.add_argument("--csv", action="store_true", help="Also save metrics to CSV")

    args = parser.parse_args()

    profiling(args.file, save_csv = args.csv)
      
