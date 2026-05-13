# importações
import pandas as pd
import ollama


# Capturando Total de valores duplicados: foco em verificar se há linhas exatamente iguais
def duplicados_por_coluna(df: pd.DataFrame) -> pd.DataFrame:
    resultado = []

    for col in df.columns:
        total_dup = df.duplicated(subset=[col]).sum()
        perc_dup = (total_dup / len(df)) * 100 if len(df) > 0 else 0

        resultado.append({
            "coluna": col,
            "duplicados": total_dup,
            "percentual": perc_dup
        })

    return pd.DataFrame(resultado)

# Criando Função que coleta as dimensões do dataframe
def dimensoes_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "Linhas": [len(df)],
        "Colunas": [len(df.columns)]
    })

# Criando Função que coleta a tipagem das colunas dos dataframes
# Nessa etapa o intuito não é saber quais colunas possuem as tipagem
# mas sim quantos de cada tipo existem
def total_por_tipagem(df: pd.DataFrame) -> pd.DataFrame:
    resultado = []

    for dtype, count in df.dtypes.value_counts().items():
        resultado.append({
            "Tipo": dtype,
            "Total": count
        })
    
    return pd.DataFrame(resultado)

# Criando Função que coleta informações sobre valores nulos
def info_nulos(df: pd.DataFrame) -> pd.DataFrame:
    dados = df.isnull().sum()
    dados['percentual'] = (dados['Total'] / len(df)) * 100  
    return dados

# Criando Função que coleta as informações de memória
def info_memoria(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "Memória": [df.memory_usage(deep=True).sum()]
    })



# Criando Funções que realizam a análise de qualidade dos dados
def introducao_qualidade_dados(caminho: str):
    return f"""
    Escreva um parágrafo introdutório para um relatório de qualidade de dados.
    
    Requisitos:
    - Tom formal e técnico
    - Máximo 3 frases
    - Mencione que o relatório apresenta métricas, problemas identificados e recomendações
    - Não use introduções genéricas como "Nos dias de hoje" ou "É sabido que"
    - O relatório se refere ao arquivo: {caminho}
    
    Formate a resposta em Markdown, com o título "## Introdução" seguido do parágrafo.
    """

def dimensoes_dados_relatorio(df: pd.DataFrame):
    valores = dimensoes_dataframe(df)
    return f"""
    Com base nas seguintes métricas dimensionais do dataframe:
    {valores}
    
    Escreva a seção "## Dimensões dos Dados" em Markdown com a seguinte estrutura:

    1. Um parágrafo introdutório curto (1-2 frases) explicando o que são as dimensões dos dados
    2. Uma tabela Markdown com as colunas: Dimensão | Valor
    3. Uma frase que mencione os valores reais e contextualize o que eles significam,
       por exemplo: volume de registros, riqueza de variáveis, escala do dataset

    Requisitos:
    - Apresente apenas os valores fornecidos, sem inferir problemas ou recomendações
    - Tom técnico e direto
    - A frase final deve ir além de apenas citar os números, agregando contexto sobre a escala e complexidade do dataset
    """

def memoria_dados_relatorio(df: pd.DataFrame):
    valores = info_memoria(df)
    return f"""
    Com base nas seguintes métricas de memória do dataframe:
    {valores}

    Escreva a seção "## Uso de Memória" em Markdown com a seguinte estrutura:

    1. Um parágrafo introdutório curto (1-2 frases) explicando o que é o uso de memória no contexto de um dataset
    2. Uma tabela Markdown com as colunas: Métrica | Valor
    3. Uma frase que mencione os valores reais e contextualize o que eles significam,
       por exemplo: eficiência de armazenamento, impacto no processamento, viabilidade de operações em memória

    Requisitos:
    - Apresente apenas os valores fornecidos, sem inferir problemas ou recomendações
    - Tom técnico e direto
    - A frase final deve ir além de apenas citar os números, agregando contexto sobre o impacto do uso de memória no processamento do dataset
    """

def relatorio(caminho: str):

    print("Iniciando leitura do arquivo...")
    dados = pd.read_csv(caminho, sep=';')
    print(f"Arquivo lido com sucesso! ({len(dados)} linhas)")

    print("Preparando prompts...")
    dim = dimensoes_dados_relatorio(dados)
    memoria = memoria_dados_relatorio(dados)
    texto = introducao_qualidade_dados(caminho)

    texto = texto + "\n\n" + dim + "\n\n" + memoria

    print("Solicitando análise ao LLM (Ollama)...")
    res = ollama.chat(
        model="deepseek-coder:6.7b",
        messages=[{"role": "user", "content": texto}]
    )

    return res['message']['content']



# Testando a função
if __name__ == "__main__":
    resultado = relatorio("12_IQVIADEZEMBRO2025CSV.csv")
    print("\n--- RELATÓRIO GERADO ---\n")
    print(resultado)
