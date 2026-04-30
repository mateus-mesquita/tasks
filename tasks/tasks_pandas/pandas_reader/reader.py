import pandas as pd
from prefect import task, get_run_logger

# Essa task serve para carregar o conjunto de dados de forma geral
@task(name = "Carregando dados via Pandas")
def carregamento_dados(caminho:str, sep = ';', encoding = 'utf-8') -> pd.DataFrame:
    logger = get_run_logger()
    try:
        logger.info(f"Iniciando carregamento do arquivo: {caminho}")
        if 'csv' in caminho:
            df = pd.read_csv(caminho, sep = sep, encoding= encoding)
        elif 'xlsx' in caminho:
            df = pd.read_excel(caminho)
        else:
            df = pd.read_parquet(caminho)
            
        logger.info(f"Arquivo carregado com sucesso. Total de linhas: {len(df)}")
        return df
        
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado no caminho: {caminho}. Erro: {e}")
        raise
    except pd.errors.EmptyDataError as e:
        logger.error(f"O arquivo está vazio: {caminho}. Erro: {e}")
        raise
    except pd.errors.ParserError as e:
        logger.error(f"Erro de parsing (verifique delimitador e formatação): {caminho}. Erro: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar o arquivo: {caminho}. Erro: {e}")
        raise

# Criando task de carregamento dos arquivos csvs por chuncks
@task(name = "Carregando arquivo via chunck")
def read_chunck(caminho:str, n_chunck:int, sep = ';', encoding = 'utf-8') -> pd.DataFrame:
    logger = get_run_logger()
    try:
        logger.info(f"Iniciando carregamento do arquivo por chunks ({n_chunck} linhas): {caminho}")
        df = pd.read_csv(caminho, sep = sep, encoding = encoding, chunksize= n_chunck)
        chuncks = [df_ for df_ in df]
        resultado = pd.concat(chuncks)
        
        logger.info(f"Carregamento por chunks concluído. Total de linhas concatenadas: {len(resultado)}")
        return resultado
        
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado no caminho: {caminho}. Erro: {e}")
        raise
    except pd.errors.EmptyDataError as e:
        logger.error(f"O arquivo está vazio: {caminho}. Erro: {e}")
        raise
    except pd.errors.ParserError as e:
        logger.error(f"Erro de parsing nos chunks (verifique delimitador e formatação): {caminho}. Erro: {e}")
        raise
    except MemoryError as e:
        logger.error(f"Memória insuficiente para concatenar os chunks do arquivo: {caminho}. Erro: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar arquivo por chunks: {caminho}. Erro: {e}")
        raise