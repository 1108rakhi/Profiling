from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from pydantic import BaseModel
import pandas as pd
from fastapi import FastAPI, Depends, Query
from typing import Optional

# Setting up databse
metrics_db_url = "mysql+mysqlconnector://root:rootroot@localhost:3306/metrics_db"  # tells sql alchemy how to connect to db
engine = create_engine(metrics_db_url, echo = True)
SessionLocal = sessionmaker(autocommit=False,autoflush=False, bind=engine)
Base = declarative_base()

#SQLAlchemy models
class Metrics(Base):
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    db_name = Column(String(255), nullable=False)       
    table_name = Column(String(255), nullable=False) 
    column_name = Column(String(255))   
    metric_type = Column(String(255), nullable=False)   
    value = Column(Float)
Base.metadata.create_all(bind=engine)


# pydantic models
class MetricCreate(BaseModel):
    db_input_url : str
    db_name : str
    table_name : str

class MetricResponse(BaseModel):
    id : int
    db_name : str
    table_name : str
    column_name : str
    metric_type : str
    value : float
    class Config:
        orm_mode = True


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def profiling_metrics(db_input_url : str, db_name : str, table_name: str):
    
    input_engine = create_engine(db_input_url)  # connecting to input db
    df = pd.read_sql(f"SELECT * FROM {table_name};", input_engine)


    total_rows = len(df)
    total_cols = df.shape[1]
    null_counts = df.isnull().sum().sum()
    null_percentage = round((null_counts/total_rows*100),2) if total_rows > 0 else 0
    
    duplicate_count = df.duplicated().sum()
    duplicate_percentage = round((duplicate_count / total_rows) * 100, 2) if total_rows > 0 else 0

    metrics = []
    metric_type = ["Total Rows", "Total Columns", "Total Null Count","Total Null %","Total Duplicate Count", "Total Duplicate %"]
    values = [total_rows, total_cols, null_counts, null_percentage, duplicate_count, duplicate_percentage]

    for m, v in zip(metric_type, values):
        metrics.append(Metrics(db_name=db_name,
                                table_name = table_name,
                                column_name ="ALL",
                                metric_type=m,
                                value=float(v)))
    null_counts_per_col = df.isnull().sum()
    if total_rows > 0:
        null_percentage_per_col = round((null_counts_per_col / total_rows * 100),2)
    else:
        null_percentage_per_col = pd.Series([0] * len(df.columns), index=df.columns)
    
    for col in df.columns:
        col_dup_count = df[col].duplicated().sum()
        col_dup_percent = round((col_dup_count / total_rows) * 100, 2) if total_rows > 0 else 0
        col_metrics = [
            ("Null Count", null_counts_per_col[col]),
            ("Null %", null_percentage_per_col[col]),
            ("Duplicate Count", col_dup_count),
            ("Duplicate %", col_dup_percent),
        ]

        for m, v in col_metrics:
            metrics.append(Metrics(db_name=db_name,
                                   table_name = table_name,
                                   column_name = col,
                                   metric_type=m,
                                   value=float(v)))   
            
    return metrics

app = FastAPI()
@app.post('/profile',response_model=list[MetricResponse])
def post_metrics(request : MetricCreate, db: Session = Depends(get_db)):
    metrics = profiling_metrics(
        db_input_url=request.db_input_url,
        db_name= request.db_name,
        table_name= request.table_name
    )
    db.add_all(metrics)
    db.commit()
    return metrics

@app.get('/metrics', response_model=list[MetricResponse])
def get_metrics(db: Session = Depends(get_db),
                db_name:Optional[str] = Query(None),
                table_name : Optional[str] = Query(None),
                column_name : Optional[str] = Query(None),
                metric_type : Optional[str] =Query(None)):
    query = db.query(Metrics)
    if db_name:
        query = query.filter(Metrics.db_name ==db_name)
    if table_name:
        query = query.filter(Metrics.table_name == table_name)
    if column_name:
        query = query.filter(Metrics.column_name ==column_name)

    if metric_type:
        query = query.filter(Metrics.metric_type == metric_type)
    return query.all()
