from reader import PandasReader
from col_num import PandasTratamentoNum
from col_str import PandasTratamentoStr

# Criando a classe
class PandasMain(PandasReader, PandasTratamentoNum, PandasTratamentoStr):
    pass

# teste

pandas_main = PandasMain()
print(pandas_main.csv("12_IQVIADEZEMBRO2025CSV.csv"))

