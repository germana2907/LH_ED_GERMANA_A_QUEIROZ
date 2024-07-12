from types import CodeType
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta
import subprocess
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_from_postgresql():
    # Execute a extração dos dados do PostgreSQL usando Embulk
    subprocess.run(['embulk', 'run', r'C:\Users\germana.a.queiroz\Challenge_1007\postgresql_config.yml'])

def extract_from_csv():
    # Execute a extração dos dados do CSV usando Embulk
    subprocess.run(['embulk', 'run', r'C:\Users\germana.a.queiroz\Challenge_1007\csv_config.yml'])

def load_to_postgresql():
    # Carregue os dados extraídos para o PostgreSQL usando Embulk
    subprocess.run(['embulk', 'run', r'C:\Users\germana.a.queiroz\Challenge_1007\postgresql_load_config.yml'])

with DAG('extract_load_pipeline',
         default_args=default_args,
         schedule=timedelta(days=1)) as dag:

    extract_from_postgresql_task = PythonOperator(
        task_id='extract_from_postgresql',
        python_callable=extract_from_postgresql
    )

    extract_from_csv_task = PythonOperator(
        task_id='extract_from_csv',
        python_callable=extract_from_csv
    )

    load_to_postgresql_task = PythonOperator(
        task_id='load_to_postgresql',
        python_callable=load_to_postgresql
    )

    extract_from_postgresql_task >> load_to_postgresql_task
    extract_from_csv_task >> load_to_postgresql_task

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped
from airflow.utils.log.logging_mixin import LoggingMixin


Base = declarative_base()

class TaskInstance(Base):
    __tablename__ = 'northwind'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    # Exemplo de uso correto com Mapped[]
    dag_model: Mapped[CodeType]  # Substitua ModelType pelo tipo correto esperado

    def __repr__(self):
        return f'<TaskInstance(id={self.id}, name={self.name})>'

class TaskInstance(Base, LoggingMixin):
    __allow_unmapped__ = True
    # restante da classe

class BaseTask(Base):
    __tablename__ = 'northwind'
    __abstract__=True

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return f'<BaseTask(id={self.id}, name={self.name})>'

class TaskInstance(BaseTask):
    __tablename__ = 'northwind'  # Substitua pelo nome da tabela no seu banco de dados

    additional_column = Column(String)

    def __repr__(self):
        return f'<TaskInstance(id={self.id}, name={self.name}, additional_column={self.additional_column})>'

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Configuração da conexão com o banco de dados
SQLALCHEMY_DATABASE_URI = 'sqlite:////C:/Users/germana.a.queiroz/airflow/airflow.db'
engine = create_engine(SQLALCHEMY_DATABASE_URI)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

