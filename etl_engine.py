import os 
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from typing import Tuple, List, Optional

# Configuração do logging Docker (Stdout)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

class DatabaseConnector:
    """
    Classe responsável por gerenciar a criação de engines de banco de dados de forma segura e padronizada.
    """
    @staticmethod
    def get_engine(driver: str, user_env: str, pass_env: str, host_env: str, port_env: str, db_env: str):
        """
        Cria uma engine SQLAlchemy montando a URL.
        """
        try:
            # Verifica se as variáveis de ambiente existem
            if not os.getenv(user_env) or not os.getenv(host_env):
                raise ValueError(f"Variáveis de ambiente para {host_env} não definidas.")

                url_object = URL.create(
                    drivername=driver,
                    username=os.getenv(user_env),
                    password=os.getenv(pass_env),
                    host=os.getenv(host_env),
                    port=int(os.getenv(port_env)),
                    database=os.getenv(db_env)
                )
                return create_engine(url_object)
        except Exception as e:
            logger.error(f"Erro ao criar engine para {db_env}: {e}")
            raise e

def extract_wordpress_orders(engine) -> pd.DataFrame:
    """
    Extrai todos os do wordpress
    """
    query = """
    SELECT