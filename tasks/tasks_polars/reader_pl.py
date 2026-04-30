import polars as pl
from prefect import task, flow, get_run_logger

# Criando task de leitura de dados usando a biblioteca polars
@task(name = "Carregando Arquivo via Polars")
def carregamento_dados_pl(caminho:str, sep = ';', encoding = 'utf-8'):
    logger = get_run_logger()
    logger.info(f"Iniciando carregamento do arquivo via Polars: {caminho}")
    try:
        if 'csv' in caminho:
            logger.info("Lendo formato CSV.")
            return pl.read_csv(caminho, separator=sep, decimal_comma=True)
        elif 'xlsx' in caminho:
            logger.info("Lendo formato Excel.")
            return pl.read_excel(caminho)
        elif 'parquet' in caminho:
            logger.info("Lendo formato Parquet.")
            return pl.read_parquet(caminho)
        else:
            logger.error(f"Formato de arquivo não suportado: {caminho}")
            raise ValueError(f"Formato de arquivo não suportado: {caminho}")
    except Exception as e:
        logger.error(f"Falha ao carregar o arquivo {caminho}: {e}")
        raise


# Criando task de leitura de dados Usando biblioteca polars
# Difente da outra função, será utilzido o processamemnto lazy
@task(name = "Carregando o Arquivo via Polars - (Processamento lazy)")
def reader_lazy(caminho:str, sep = ';'):
    logger = get_run_logger()
    logger.info(f"Iniciando carregamento lazy do arquivo via Polars: {caminho}")
    try:
        if 'csv' in caminho:
            logger.info("Lendo formato CSV (Lazy).")
            dados = pl.scan_csv(caminho, separator=sep)
            return dados.collect()
        elif 'xlsx' in caminho:
            logger.info("Lendo formato Excel (Eager fallback).")
            dados = pl.read_excel(caminho)
            return dados
        elif 'parquet' in caminho:
            logger.info("Lendo formato Parquet (Lazy).")
            dados = pl.scan_parquet(caminho)
            return dados.collect()
        else:
            logger.error(f"Formato de arquivo não suportado: {caminho}")
            raise ValueError(f"Formato de arquivo não suportado: {caminho}")
    except Exception as e:
        logger.error(f"Falha ao carregar o arquivo em modo lazy {caminho}: {e}")
        raise