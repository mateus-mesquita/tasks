from reader import PandasReader
from col_num import PandasTratamentoNum, PandasTratamentoStr
from prefect import task, flow
import pandas as pd
from rich import print as rprint


# Criando a classe
class PandasMain(PandasReader, PandasTratamentoNum, PandasTratamentoStr):
    pass

# Construindo Fluxos pandas para cada tipo de arquivo:
class PandasCSV():
    def __init__(self):
        self.pandas_main = PandasMain()

    # Camada de broze
    @task(name = "Carregando arquivo csv")
    def csv_raw(self, path:str, sep = ';', encoding='utf-8', dtype = None, header = 0) -> pd.DataFrame:
        dados = self.pandas_main.csv(path, sep, encoding, dtype, header)
        return dados
    
    # Camada de silver
    @flow(name = "Tratamento de colunas numéricas")
    def csv_silver(self, path:str) -> pd.DataFrame:
        # Carregando dados csv: 
        dados = self.pandas_main.csv(path)
        # Verificando colunas numéricas: 
        colunas = self.pandas_main.verificar_cols_num(dados)
        
        # Verificando colunas numéricas mascaradas: 
        colunas_mask = self.pandas_main.verificar_cols_num_mask(dados, colunas)
        dados = self.pandas_main.tratar_cols_num_mask(dados, colunas_mask)
        
        # Tratando colunas numéricas: 
        dados = self.pandas_main.tratar_cols_num(dados, colunas)
        # Verificando espacamento: 
        colunas = self.pandas_main.verificar_espacamento(dados)
        dados = self.pandas_main.remover_espacamento(dados, colunas)
        return dados



    
