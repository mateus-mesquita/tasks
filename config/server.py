import subprocess
from prefect.logging import get_logger

def server_start(func):
    def decorador(*args, **kwargs):
        logger = get_logger("server_setup")
        try:
            logger.info("Iniciando o processo de configuração do servidor Prefect...")
            comando_config = 'prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"'
            
            logger.info("Abrindo o PowerShell para executar 'prefect server start' em segundo plano...")
            subprocess.Popen('start powershell -NoExit -Command "prefect server start"', shell=True)
            
            logger.info("Configurando a variável de ambiente PREFECT_API_URL...")
            subprocess.run(["powershell", "-Command", comando_config], check=True)
            
            logger.info("Servidor configurado. Executando a função principal...")
            return func(*args, **kwargs)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Erro ao executar comando no PowerShell (Verifique configurações do Prefect): {e}")
            raise
        except FileNotFoundError as e:
            logger.error(f"Executável não encontrado (O PowerShell ou as variáveis de ambiente podem não estar configurados no PATH): {e}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado na configuração do servidor: {e}")
            raise
            
    return decorador