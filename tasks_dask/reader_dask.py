from prefect import task, get_run_logger
import dask.dataframe as dd


# Criando task de Carregamento geral de dados usando Dask
@task(name = "Carregando arquivos via Dask")
def carregamento_dados_dask(caminho:str, sep = ';'):
    logger = get_run_logger()
    logger.info(f"Iniciando carregamento do arquivo via Dask: {caminho}")
    try:
        if 'csv' in caminho:
            logger.info("Lendo formato CSV.")
            return dd.read_csv(caminho, sep = sep)
        elif 'xlsx' in caminho:
            logger.info("Lendo formato Excel (Atenção: Dask não possui leitura nativa robusta para Excel).")
            ...
        elif 'parquet' in caminho:
            logger.info("Lendo formato Parquet.")
            return dd.read_parquet(caminho)
        else:
            logger.error(f"Formato de arquivo não suportado: {caminho}")
            raise ValueError(f"Formato de arquivo não suportado: {caminho}")
    except Exception as e:
        logger.error(f"Falha ao carregar o arquivo {caminho}: {e}")
        raise