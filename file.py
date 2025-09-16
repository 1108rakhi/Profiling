import sqlite3
import mysql.connector
import pandas as pd
import argparse

def profiling_db(host, user, password, database, table_name, save_csv = False, save_db = False ):
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = database
    )
    query = f'SELECT * FROM {table_name};'
    df = pd.read_sql(query, conn)
    conn.close()

    total_rows = len(df)
    total_cols = df.shape[1]
    null_counts = df.isnull().sum()
    null_percentage = round((null_counts / total_rows) * 100, 2)
    duplicate_count = df.duplicated().sum()
    duplicate_percentage = round((duplicate_count / total_rows) * 100, 2)

# CLI Output 
    print(f"\nProfiling Report for table: {table_name}")
    print(' Total rows:', total_rows)
    print(' Total columns:', total_cols)

    for col in df.columns:
        col_dup_count = df[col].duplicated().sum()
        col_dup_percent = round((col_dup_count / total_rows) * 100, 2)
        print(f"\nColumn: {col}")
        print(f"  Null Count: {null_counts[col]}")
        print(f"  Null %: {null_percentage[col]}%")
        print(f"  Duplicate Count: {col_dup_count}")
        print(f"  Duplicate %: {col_dup_percent}%")  

# CSV Output
    metrics = []
    if save_csv:
        metric_type = ["Total Rows", "Total Columns", "Row-wise Duplicate Count", "Row-wise Duplicate %"]
        values = [total_rows, total_cols, duplicate_count, duplicate_percentage]
        metrics.extend({"Metric Type": m, "Value": v} for m, v in zip(metric_type, values))

        for col in df.columns:
            col_dup_count = df[col].duplicated().sum()
            col_dup_percent = round((col_dup_count / total_rows)*100, 2)
            metric_types = [
                f"Null Count [{col}]",
                f"Null % [{col}]",
                f"Duplicate Count [{col}]",
                f"Duplicate % [{col}]"
            ] 
            values = [null_counts[col], null_percentage[col], col_dup_count, col_dup_percent]
            metrics.extend({"Metric Type": m, "Value": v} for m, v in zip(metric_types, values))

        report = pd.DataFrame(metrics)
        output_file = f"{table_name}_profiling_report.csv"
        report.to_csv(output_file, index=False)
        print("csv saved")

# DB Output
    if save_db:
        conn_sqlite = sqlite3.connect('profiling_metrics.db')
        cursor = conn_sqlite.cursor()
        #cursor.execute("DROP TABLE IF EXISTS metrics")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT,
                metric_type TEXT,
                value REAL
            )
        """)
        rows = [(table_name, m["Metric Type"], m["Value"]) for m in metrics]
        cursor.executemany(
            "INSERT INTO metrics (table_name, metric_type, value) VALUES (?, ?, ?)", rows
        )
        conn_sqlite.commit()
        conn_sqlite.close()
        print('Metrics saved in db')


# # Read from DB
# def read_db(table_name=None):
#     conn_sqlite = sqlite3.connect('profiling_metrics.db') 
#     cursor = conn_sqlite.cursor()
#     if table_name:
#         cursor.execute(
#             "SELECT table_name, metric_type, value FROM metrics WHERE table_name = ?", (table_name,)
#         )
#     else:
#         cursor.execute("SELECT table_name, metric_type, value FROM metrics")

#     rows = cursor.fetchall()
#     conn_sqlite.close()
#     data = pd.DataFrame(rows, columns=['Table Name', 'Metric Type', 'Value'])
#     print('Data read from DB: \n', data)
#     return data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MySQL Table Profiling")
    parser.add_argument("table", help="Name of the MySQL table to profile")
    parser.add_argument("--host", default="localhost", help="MySQL host")
    parser.add_argument("--user", required=True, help="MySQL username")
    parser.add_argument("--password", required=True, help="MySQL password")
    parser.add_argument("--database", required=True, help="MySQL database name")
    parser.add_argument("--csv", action="store_true", help="Save metrics to CSV")
    parser.add_argument("--dbout", action="store_true", help="Save metrics to SQLite DB")

    args = parser.parse_args()

    profiling_db(
            host=args.host,
            user=args.user,
            password=args.password,
            database=args.database,
            table_name=args.table,
            save_csv=args.csv,
            save_db=args.dbout
        )