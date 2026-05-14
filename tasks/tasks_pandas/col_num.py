import pandas as pd
import numpy as np
from prefect import task
from rich import print as rprint

# alterando a lógica para classes
class PandasTratamentoNum:
    @task(name = "Verificando colunas numéricas")
    def verificar_cols_num(self, dados:pd.DataFrame) -> list:
        lista = []
        try:
            for col in dados.columns:
                if not pd.api.types.is_numeric_dtype(dados[col]):
                    lista.append(col)
            return lista
        except Exception as e:
            rprint(f"[bold red]Erro ao verificar colunas numéricas:[/bold red] [yellow]{e}[/yellow]")
    
    @task(name = "Verificando colunas numéricas mascaradas")
    def verificar_cols_num_mask(self, dados:pd.DataFrame, colunas:list) -> list:
        lista = []
        try:
            for col in colunas:
                if dados[col].str.match(r"^[0-9\\,]+$").all():
                    lista.append(col)
            return lista
        except Exception as e:
            rprint(f"[bold red]Erro ao verificar colunas numéricas mascaradas:[/bold red] [yellow]{e}[/yellow]")
    
    @task(name = "Tratando colunas numéricas mascaradas")
    def tratar_cols_num_mask(self, dados:pd.DataFrame, colunas:list) -> pd.DataFrame:
        try:
            for col in colunas:
                dados[col] = dados[col].str.replace('.', '', regex=False)
                dados[col] = dados[col].str.replace(',', '.', regex=False)
            return dados
        except Exception as e:
            rprint(f"[bold red]Erro ao tratar colunas numéricas mascaradas:[/bold red] [yellow]{e}[/yellow]")
    
    @task(name = "Tratando colunas numéricas")
    def tratar_cols_num(self, dados:pd.DataFrame, colunas:list) -> pd.DataFrame:
        try:
            for col in colunas:
                dados[col] = pd.to_numeric(dados[col], errors='coerce')
            return dados
        except Exception as e:
            rprint(f"[bold red]Erro ao tratar colunas numéricas:[/bold red] [yellow]{e}[/yellow]")

class PandasTratamentoStr:
    @task(name = "Verificando espacamento")
    def verificar_espacamento(self, dados:pd.DataFrame) -> list:
        lista = []
        try:
            for col in dados.columns:
                if dados[col].dtype == 'object' or dados[col].dtype == 'string':
                    if True in dados[col].str.contains(r" ", na=False):
                        lista.append(col)
            return lista
        except Exception as e:
            rprint(f"[bold red]Erro na task verificando_espacamento:[/bold red] [yellow]{e}[/yellow]")
            raise
    
    @task(name = "Removendo espacamento")
    def remover_espacamento(self, dados:pd.DataFrame, colunas:list) -> pd.DataFrame:
        try:
            for col in colunas:
                dados[col] = dados[col].str.replace(r" ", "", regex=False)
            return dados
        except Exception as e:
            rprint(f"[bold red]Erro na task remover_espacamento:[/bold red] [yellow]{e}[/yellow]")
            raise