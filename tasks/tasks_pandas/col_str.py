# importações 
import pandas as pd
# alterando a lógica para classes
class PandasTratamentoStr:
    def verificar_espacamento(self, dados:pd.DataFrame) -> list:
        lista = []
        try:
            for col in dados.columns:
                if dados[col].dtype == 'object' or dados[col].dtype == 'string':
                    if True in dados[col].str.contains(r" ", na=False):
                        lista.append(col)
            return lista
        except Exception as e:
            print(f"Erro na task verificando_espacamento: {e}")
            raise
    
    def remover_espacamento(self, dados:pd.DataFrame, colunas:list) -> pd.DataFrame:
        try:
            for col in colunas:
                dados[col] = dados[col].str.replace(r" ", "", regex=False)
            return dados
        except Exception as e:
            print(f"Erro na task remover_espacamento: {e}")
            raise
