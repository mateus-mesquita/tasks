import polars as pl
from prefect import task, flow, get_run_logger

# Criando Classe de tasks de carregamento de dados usando polars
class PolarsReader:
    @task(name = "Carregando Arquivo via Polars")
    def csv(self, caminho:str, sep = ';', encoding = 'utf-8'):
        logger = get_run_logger()
        try:
            return pl.read_csv(caminho, separator=sep, decimal_comma=True)
        except Exception as e:
            logger.error(f"Erro ao carregar CSV {caminho}: {e}")
            raise

    @task(name = "Carregando Arquivo Excel via Polars")
    def excel(self, caminho:str, sheet_name:str = None):
        logger = get_run_logger()
        try:
            return pl.read_excel(caminho, sheet_name=sheet_name)
        except Exception as e:
            logger.error(f"Erro ao carregar Excel {caminho}: {e}")
            raise
    
    @task(name = "Carregando Arquivo Parquet via Polars")
    def parquet(self, caminho:str):
        logger = get_run_logger()
        try:
            return pl.read_parquet(caminho)
        except Exception as e:
            logger.error(f"Erro ao carregar Parquet {caminho}: {e}")
            raise


# Criando Classe de Processamento lazy usando polars
class PolarsReaderLazy:
    @task(name = "Carregando Arquivo CSV lazy via Polars")
    def csv(self, caminho:str, sep = ';', encoding = 'utf-8'):
        logger = get_run_logger()
        try:
            return pl.scan_csv(caminho, separator=sep).collect()
        except Exception as e:
            logger.error(f"Erro ao carregar CSV lazy {caminho}: {e}")
            raise
    
    @task(name = "Carregando Arquivo Excel lazy via Polars")
    def excel(self, caminho:str, sheet_name:str = None):
        logger = get_run_logger()
        try:
            return pl.read_excel(caminho, sheet_name=sheet_name)
        except Exception as e:
            logger.error(f"Erro ao carregar Excel lazy {caminho}: {e}")
            raise
    
    @task(name = "Carregando Arquivo Parquet lazy via Polars")
    def parquet(self, caminho:str):
        logger = get_run_logger()
        try:
            return pl.scan_parquet(caminho)
        except Exception as e:
            logger.error(f"Erro ao carregar Parquet lazy {caminho}: {e}")
            raise

