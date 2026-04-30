from prefect import flow, get_run_logger
import pandas as pd
from config.server import server_start
from tasks_pandas.pandas_reader.reader import carregamento_dados, read_chunck
from tasks_pandas.tratamento.col_num import verificar_cols_num, verificar_cols_num_mask, tratar_cols_num_mask, tratar_cols_num
from tasks_pandas.tratamento.col_str import verificando_espacamento, remover_espacamento
from tasks_pandas.tratamento.duplicas import identificacao_remocao_colunas_duplicadas


# Criando Fluxo de Tratamento de dados via pandas
# focando em processamento de grandes volumes de dados

@server_start
@flow(name="Fluxo de Tratamento de dados via pandas")
def flow_pandas(caminho:str, n_chunck=None, sep = ';', encoding = 'utf-8') -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Iniciando o fluxo de tratamento de dados para o arquivo: {caminho}")

    if n_chunck is None:
        logger.info("Carregando arquivo inteiro na memória (n_chunck is None).")
        df = carregamento_dados(caminho, sep, encoding)
    else:
        logger.info(f"Carregando arquivo em blocos (chunks) de tamanho: {n_chunck}.")
        df = read_chunck(caminho, n_chunck, sep, encoding)
    
    # Iniciando o Tratamento dos Dados: foco nas colunas númericas
    logger.info("Iniciando o tratamento de dados das colunas numéricas.")
    colunas = verificar_cols_num(df) # Coletando as colunas que não são numéricas
    colunas_mask = verificar_cols_num_mask(df, colunas) # Verificando se as colunas não numéricas são numéricas mascaradas como texto
    df = tratar_cols_num_mask(df, colunas_mask) # Tratando as colunas numéricas mascaradas como texto
    df = tratar_cols_num(df, colunas_mask) # Tratando as colunas numéricas

    # Iniciando o Tratamento dos Dados: foco nas colunas categóricas
    logger.info("Iniciando o tratamento de dados das colunas categóricas (remoção de espaços).")
    colunas_str = verificando_espacamento(df) # Verificando se as colunas não numéricas tem espaçamento
    df = remover_espacamento(df, colunas_str) # Removendo espaçamento das colunas não numéricas

    logger.info("Iniciando o tratamento de dados das colunas duplicadas.")
    df = identificacao_remocao_colunas_duplicadas(df) # Identificando e removendo colunas duplicadas

    logger.info("Fluxo de tratamento finalizado com sucesso.")
    return df

