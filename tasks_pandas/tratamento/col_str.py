# importações 
import pandas as pd
from prefect import task, flow, get_run_logger

# Criando task que identifica colunas não numéricas
@task(name = "Tratando espaçamento nos dados")
def verificando_espacamento(dados:pd.DataFrame) -> list:
    logger = get_run_logger()
    lista = []
    try:
        for col in dados.columns:
            if dados[col].dtype == 'object' or dados[col].dtype == 'string':
                if True in dados[col].str.contains(r" ", na=False):
                    lista.append(col)
        return lista
    except Exception as e:
        logger.error(f"Erro na task verificando_espacamento: {e}")
        raise

#Criando task que remove espaçamento
@task(name = "Removendo espaçamento nos dados")
def remover_espacamento(dados:pd.DataFrame, colunas:list) -> pd.DataFrame:
    logger = get_run_logger()
    col_atual = None
    try:
        for col in colunas:
            col_atual = col
            dados[col] = dados[col].str.replace(r" ", "", regex=False)
        return dados
    except KeyError as e:
        logger.error(f"Erro: Coluna '{col_atual}' não encontrada no DataFrame. {e}")
        raise
    except AttributeError as e:
        logger.error(f"Erro: Falha ao aplicar replace na coluna '{col_atual}'. {e}")
        raise
    except Exception as e:
        logger.error(f"Erro na task remover_espacamento (coluna: '{col_atual}'): {e}")
        raise