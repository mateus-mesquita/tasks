import pandas as pd

# Alterando a lógica para classes
class PandasReader:  
    def csv(self, path: str, sep = ';', encoding='utf-8', dtype = None, header = 0) -> pd.DataFrame:
        try:
            dados = pd.read_csv(
                path,
                sep = sep,
                encoding= encoding,
                dtype = dtype,
                header = header
            )
            return dados
        except Exception as e:
            print(f"Erro ao carregar arquivo csv: {e}")
    
    def excel(self, path: str, sheet_name:str = None) -> pd.DataFrame:
        try:
            dados = pd.read_excel(path, sheet_name=sheet_name)
            return dados
        except Exception as e:
            print(f"Erro ao carregar arquivo excel: {e}")
    
    def parquet(self, path: str) -> pd.DataFrame:
        try:
            dados = pd.read_parquet(path)
            return dados
        except Exception as e:
            print(f"Erro ao carregar arquivo parquet: {e}")