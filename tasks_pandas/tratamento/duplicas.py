import pandas as pd
from prefect import task, get_run_logger

# Criando task que identifica colunas duplicadas e remove
@task(name = "Identificação e remoção de colunas duplicadas")
def identificacao_remocao_colunas_duplicadas(dados:pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        total = dados.duplicated().sum()
        if total > 0:
            dados = dados.drop_duplicates()
            logger.info(f"Foram encontradas e removidas {total} colunas duplicadas.")
        else:
            logger.info("Não foram encontradas colunas duplicadas.")
        return dados
    except Exception as e:
        logger.error(f"Erro inesperado na task identificacao_remocao_colunas_duplicadas: {e}")
        raise