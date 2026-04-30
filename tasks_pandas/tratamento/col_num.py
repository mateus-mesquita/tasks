import pandas as pd
import numpy as np
from prefect import task, flow, get_run_logger

# Criando task que verifica se uma coluna é ou não numérica
# essa task tem como objetivo verificar a existência de colunas
# numéricas mascaradas como object

@task(name = "Verificação de colunas numéricas")
def verificar_cols_num(dados:pd.DataFrame) -> list:
    logger = get_run_logger()
    lista = []
    try:
        for col in dados.columns:
            if not pd.api.types.is_numeric_dtype(dados[col]):
                lista.append(col)
        return lista
    except AttributeError as e:
        logger.error(f"Erro: O objeto passado não possui o atributo 'columns'. Certifique-se de passar um DataFrame válido. Detalhes: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado na task verificar_cols_num: {e}")
        raise

@task(name = "Identificando Colunas Númericas Mascaradas Como Texto")
def verificar_cols_num_mask(dados:pd.DataFrame, colunas:list) -> list:
    logger = get_run_logger()
    lista = []
    col_atual = None
    try:
        for col in colunas:
            col_atual = col
            if dados[col].str.match(r"^[0-9\,]+$").all():
                lista.append(col)
        return lista
    except KeyError as e:
        logger.error(f"Erro: A coluna '{col_atual}' não foi encontrada no DataFrame. Detalhes: {e}")
        raise
    except AttributeError as e:
        logger.error(f"Erro: Falha ao aplicar regex na coluna '{col_atual}'. Pode não ser do tipo string. Detalhes: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado na task verificar_cols_num_mask (coluna: '{col_atual}'): {e}")
        raise

# Criando Task que realiza o tratamento de . para , 
@task(name = "Tratamento de Colunas Numéricas Mascaradas Como Texto")
def tratar_cols_num_mask(dados:pd.DataFrame, colunas:list) -> pd.DataFrame:
    logger = get_run_logger()
    col_atual = None
    try:
        for col in colunas:
            col_atual = col
            dados[col] = dados[col].str.replace('.', '', regex=False)
            dados[col] = dados[col].str.replace(',', '.', regex=False)
        return dados
    except KeyError as e:
        logger.error(f"Erro: A coluna '{col_atual}' não foi encontrada no DataFrame. Detalhes: {e}")
        raise
    except AttributeError as e:
        logger.error(f"Erro: Falha ao aplicar replace na coluna '{col_atual}'. Pode não ser do tipo string. Detalhes: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado na task tratar_cols_num_mask (coluna: '{col_atual}'): {e}")
        raise

# Criando task que realiza o tratamento de str para numério
@task(name = "Tratamento de Colunas Numéricas")
def tratar_cols_num(dados:pd.DataFrame, colunas:list) -> pd.DataFrame:
    logger = get_run_logger()
    col_atual = None
    try:
        for col in colunas:
            col_atual = col
            dados[col] = pd.to_numeric(dados[col], errors='coerce')
        return dados
    except KeyError as e:
        logger.error(f"Erro: A coluna '{col_atual}' não foi encontrada no DataFrame. Detalhes: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado na task tratar_cols_num (coluna: '{col_atual}'): {e}")
        raise

