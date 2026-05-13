import pandas as pd
import numpy as np
# alterando a lógica para classes
class PandasTratamentoNum:
    def verificar_cols_num(self, dados:pd.DataFrame) -> list:
        lista = []
        try:
            for col in dados.columns:
                if not pd.api.types.is_numeric_dtype(dados[col]):
                    lista.append(col)
            return lista
        except Exception as e:
            print(f"Erro ao verificar colunas numéricas: {e}")
    
    def verificar_cols_num_mask(self, dados:pd.DataFrame, colunas:list) -> list:
        lista = []
        try:
            for col in colunas:
                if dados[col].str.match(r"^[0-9\\,]+$").all():
                    lista.append(col)
            return lista
        except Exception as e:
            print(f"Erro ao verificar colunas numéricas mascaradas: {e}")
    
    def tratar_cols_num_mask(self, dados:pd.DataFrame, colunas:list) -> pd.DataFrame:
        try:
            for col in colunas:
                dados[col] = dados[col].str.replace('.', '', regex=False)
                dados[col] = dados[col].str.replace(',', '.', regex=False)
            return dados
        except Exception as e:
            print(f"Erro ao tratar colunas numéricas mascaradas: {e}")
    
    def tratar_cols_num(self, dados:pd.DataFrame, colunas:list) -> pd.DataFrame:
        try:
            for col in colunas:
                dados[col] = pd.to_numeric(dados[col], errors='coerce')
            return dados
        except Exception as e:
            print(f"Erro ao tratar colunas numéricas: {e}")