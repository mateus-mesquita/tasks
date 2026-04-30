from prefect import flow
import pandas as pd
from tasks_pandas.pandas_reader import carregamento_dados, read_chunck
from tasks_pandas.tratamento.col_num import verificar_cols_num, verificar_cols_num_mask, tratar_cols_num_mask, tratar_cols_num



# Criando Fluxo de Tratamento de dados via pandas
# focando em processamento de grandes volumes de dados

@flow(name = " Fluxo de Tratamento de dados via pandas")
def flow_pandas(caminho:str, n_chunck:None, sep = ';', encoding = 'utf-8') -> pd.DataFrame:
    if n_chunck is None:
        df = carregamento_dados(caminho, sep, encoding)
    else:
        df = read_chunck(caminho, n_chunck, sep, encoding)
    
    # Iniciando o Tratamento dos Dados: foco nas colunas númericas
    colunas = verificar_cols_num(df) # Coletando as colunas que não são numéricas
    colunas_mask = verificar_cols_num_mask(df, colunas) # Verificando se as colunas não numéricas são numéricas mascaradas como texto
    df = tratar_cols_num_mask(df, colunas_mask) # Tratando as colunas numéricas mascaradas como texto
    df = tratar_cols_num(df, colunas) # Tratando as colunas numéricas

    return df


    