from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine


def importar_aracaju():

    # Ler CSV
    df = pd.read_csv(
        "/opt/airflow/arquivos/dados_processo_seletivo.csv"
    )

    # Filtrar apenas Aracaju
    df_aracaju = df[df["city"] == "ARACAJU"]

    df_aracaju = df_aracaju.rename(
    columns={"Unnamed: 0": "num"}
     )  

    print(f"Encontrados {len(df_aracaju)} registros de Aracaju")

    # Conexão com PostgreSQL
    engine = create_engine(
        "postgresql://admin:123456@postgres:5432/mydb"
    )

    # Inserir na tabela
    df_aracaju.to_sql(
        name="aracaju",
        con=engine,
        if_exists="append",
        index=False
    )

    print("Dados inseridos com sucesso!")


with DAG(
    dag_id="importa_aracaju",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    tarefa_importar = PythonOperator(
        task_id="importar_csv",
        python_callable=importar_aracaju
    )