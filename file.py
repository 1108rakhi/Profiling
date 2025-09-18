from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from pydantic import BaseModel
import pandas as pd

Base = declarative_base()

class Metrics(Base):
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    db_name = Column(String(255), nullable=False)       
    table_name = Column(String(255), nullable=False)    
    metric_type = Column(String(255), nullable=False, )   
    value = Column(Float, nullable=False)

class MetricModel(BaseModel):
    db_name : str
    table_name : str
    metric_type : str
    value : float


def profiling(db_input_url : str, db_output_url : str, db_name : str, table_name: str ):
    
    input_engine = create_engine(db_input_url)
    df = pd.read_sql(f'SELECT * FROM {table_name};', input_engine)

    total_rows = len(df)
    total_cols = df.shape[1]
    null_counts = df.isnull().sum()
    null_percentage = round((null_counts / total_rows) * 100, 2) if total_rows > 0 else 0
    duplicate_count = df.duplicated().sum()
    duplicate_percentage = round((duplicate_count / total_rows) * 100, 2) if total_rows > 0 else 0

    metrics = []
    metric_type = ["Total Rows", "Total Columns", "Row-wise Duplicate Count", "Row-wise Duplicate %"]
    values = [total_rows, total_cols, duplicate_count, duplicate_percentage]

    for m, v in zip(metric_type, values):
        metrics.append(Metrics(db_name=db_name,
                                   table_name = table_name,
                                   metric_type=m,
                                   value=float(v)))
        
    for col in df.columns:
        col_dup_count = df[col].duplicated().sum()
        col_dup_percent = round((col_dup_count / total_rows) * 100, 2) if total_rows > 0 else 0
        col_metrics = [
            ("Null Count", null_counts[col]),
            ("Null %", null_percentage[col]),
            ("Duplicate Count", col_dup_count),
            ("Duplicate %", col_dup_percent),
        ]
        for m, v in col_metrics:
            metrics.append(Metrics(db_name=db_name,
                                   table_name = table_name,
                                   metric_type=f'{m} [{col}]',
                                   value=float(v)))   

    output_engine = create_engine(db_output_url)
    Base.metadata.create_all(output_engine)
    Session = sessionmaker(bind=output_engine)
    session=Session()

    session.add_all(metrics)
    session.commit()
    session.close()
    print(f'Metrics for table - {table_name} saved')

if __name__ == '__main__':
    db_input_url = "mysql+mysqlconnector://root:rootroot@localhost/training"
    db_output_url = "mysql+mysqlconnector://root:rootroot@localhost/metrics_db"

    profiling(db_input_url, db_output_url, db_name="training", table_name="sales_rep")    
            

