import sqlite3
import pandas as pd

# Conexão com a base de dados SQLite
conn = sqlite3.connect("mental_health.sqlite")

# Lista de tabelas
tabelas = ['Answer', 'Question', 'Survey']
arquivos_csv = []

for tabela in tabelas:
    # Leitura de cada tabela para um DataFrame
    df = pd.read_sql_query(f"SELECT * FROM {tabela}", conn)
    # Nome do arquivo CSV
    csv_file = f"{tabela}.csv"
    arquivos_csv.append(csv_file)
    # Salvar o DataFrame como CSV
    df.to_csv(csv_file, index=False)


conn.close()