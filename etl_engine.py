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
    Extrai pedidos utilizando a estrutura HPOS (High-Performance Order Storage)
    e cruza com dados de Pagar.me e Clientes.
    """
    query = """
    SELECT 
        o.id AS pedido_id,
        o.date_created_gmt AS data_pedido,
        o.total_amount AS valor_total,
        o.status AS status_woo,
        
        -- Dados do Cliente
        u.user_email AS email_cliente,
        CONCAT(addr.first_name, ' ', addr.last_name) AS nome_cliente,
        addr.city AS cidade,
        addr.state AS estado,
        addr.phone AS telefone,

        -- Dados Financeiros (Pagar.me V5)
        pg.transaction_id AS tid_pagarme,
        pg.installments AS parcelas,
        pg.payment_method AS metodo_pagamento,
        pg.status AS status_pagamento

    FROM 
        wp_wc_orders o
    
    -- Join com Tabela de Usuários (pode ser NULL se for compra como visitante)
    LEFT JOIN 
        wp_users u ON o.customer_id = u.ID
    
    -- Join com Endereço de Cobrança (Billing)
    LEFT JOIN 
        wp_wc_order_addresses addr ON o.id = addr.order_id AND addr.address_type = 'billing'
    
    -- Join com Transações Pagar.me
    LEFT JOIN
        wp_pagarme_module_core_transaction pg ON o.id = pg.order_id

    ORDER BY 
        o.date_created_gmt DESC
    LIMIT 5000;
    """
    return pd.read_sql(query, engine)

def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa e padroniza os dados extraídos.
    """
    if df.empty:
        return df
    
    # 1. Normalização de Status (remove os prefixos wc-)
    df['status_woo'] = df['status_woo'].str.replace('wc-', '', regex=False)

    # 2. Conversão de Tiops Numéricos
    # 'errors=coerce' transforma erros em NaN, depois preenche com 0.0
    df['valor_total'] = pd.to_numeric(df['valor_total'], errors='coerce').fillna(0.0)
    df['parcelas'] = pd.to_numeric(df['parcelas'], errors='coerce').fillna(1).astype(int)

    # 3. Conversão de Datas
    df['data_pedido'] = pd.to_datetime(df['data_pedido'])

    # 4. Tratamento de Valores Nulos
    df['valor_total'] = pd.to_numeric(df['valor_total'], errors='coerce').fillna(0.0)
    df['parcelas'] = pd.to_numeric(df['parcelas'], errors='coerce').fillna(1).astype(int)

    return df

def run_etl() -> Tuple[bool, List[str]]:
    """
    Função principal para executar o processo ETL.
    """
    ui_logs = []

    def log_msg(message: str, level='info'):
        if level == 'info':
            logger.info(message)
            ui_logs.append(f"INFO: {message}")
        elif level == 'error':
            logger.error(message)
            ui_logs.append(f"ERROR: {message}")
        elif level == 'success':
            logger.info(message)
            ui_logs.append(f"SUCCESS: {message}")

    try:
        log_msg("Iniciando processo ETL de pedidos do WordPress.")

        # 1. conexões com bancos de dados
        engine_wp = DatabaseConnector.get_engine(
            "mysql+pymysql", "WP_USER", "WP_PASS", "WP_HOST", "WP_PORT", "WP_DB_NAME"
        )
        
        engine_dw = DatabaseConnector.get_engine(
            "postgresql", "POSTGRES_USER", "POSTGRES_PASSWORD", "dw", "5432", "POSTGRES_DB"
        )

        # 2. Pipeline Principal
        log_msg("Extraindo dados consolidados (Pedidos + Clientes + Financeiro)...")
        df_raw = extract_wordpress_orders(engine_wp)

        if not df_raw.empty:
            log_msg(f"{len(df_raw)} linhas extraídas com sucesso. Tratamento dos dados em andamento...")

            df_clean = transform_orders(df_raw)
            log_msg("Dados tratados com sucesso. Iniciando carga no Data Warehouse...")
            df_clean.to_sql('pedidos_consolidados', engine_dw, if_exists='replace', index=False, chunksize=1000)

            log_msg(f"Carga concluída com sucesso. Foram adicionados {len(df_clean)} registros no DW.", level='success')

        else:
            log_msg("Nenhum pedido encontrado na tabela wp_wc_orders.", level = "info")

        return True, ui_logs

    except Exception as e:
        log_msg(f"Erro Crítico no ETL: {str(e)}", "error")
        return False, ui_logs