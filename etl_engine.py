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
    Design Pattern: Factory Method simples para conexões.
    """
    @staticmethod
    def get_engine(driver: str, user_env: str, pass_env: str, host_env: str, port_env: str, db_env: str):
        """
        Cria uma engine SQLAlchemy montando a URL.
        
        Args:
            driver (str): Driver do banco (ex: 'mysql+pymysql', 'postgresql')
            user_env (str): NOME da variável de ambiente do usuário
            pass_env (str): NOME da variável de ambiente da senha
            host_env (str): NOME da variável de ambiente do host
            port_env (str): NOME da variável de ambiente da porta
            db_env (str): NOME da variável de ambiente do nome do banco
        """
        try:
            # Recupera valores
            user = os.getenv(user_env)
            password = os.getenv(pass_env)
            host = os.getenv(host_env)
            db_name = os.getenv(db_env)
            
            # Tratamento seguro para porta: se não achar, usa 3306/5432 padrão ou falha graciosamente
            port_str = os.getenv(port_env)
            
            # Validação básica
            if not user or not host or not password:
                # Loga qual variável está faltando para facilitar debug (sem mostrar a senha)
                missing = []
                if not user: missing.append(user_env)
                if not host: missing.append(host_env)
                if not password: missing.append(pass_env)
                raise ValueError(f"Variáveis de ambiente obrigatórias não definidas: {', '.join(missing)}")

            # Define porta padrão caso a env não exista (fallback)
            port = int(port_str) if port_str and port_str.isdigit() else 3306

            url_object = URL.create(
                drivername=driver,
                username=user,
                password=password,
                host=host,
                port=port,
                database=db_name
            )
            return create_engine(url_object)
            
        except Exception as e:
            logger.error(f"Erro crítico ao criar engine para variável {db_env}: {str(e)}")
            raise e

def extract_wordpress_orders(engine) -> pd.DataFrame:
    """
    Extrai pedidos utilizando a estrutura HPOS (High-Performance Order Storage)
    e cruza com dados de Pagar.me e Clientes.
    """
    # NOTE: O LIMIT 5000 é útil para dev/homologação. 
    # TODO: Para produção, remover o LIMIT ou implementar carga incremental (filtro por data).
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
    
    # Cria uma cópia para evitar SettingWithCopyWarning do Pandas em alguns contextos
    df = df.copy()

    # 1. Normalização de Status (remove os prefixos wc-)
    # Verifica se a coluna existe antes de tentar processar
    if 'status_woo' in df.columns:
        df['status_woo'] = df['status_woo'].str.replace('wc-', '', regex=False)

    # 2. Conversão de Tipos Numéricos
    # 'errors=coerce' transforma erros em NaN, depois preenchemos.
    df['valor_total'] = pd.to_numeric(df['valor_total'], errors='coerce').fillna(0.0)
    df['parcelas'] = pd.to_numeric(df['parcelas'], errors='coerce').fillna(1).astype(int)

    # 3. Conversão de Datas
    df['data_pedido'] = pd.to_datetime(df['data_pedido'])

    # 4. Tratamento Estético de Nulos (Strings)
    # Importante para dashboards não exibirem "None" ou ficarem em branco
    cols_texto = ['metodo_pagamento', 'nome_cliente', 'cidade', 'estado']
    for col in cols_texto:
        if col in df.columns:
            df[col] = df[col].fillna('Não Informado')

    return df

def run_etl() -> Tuple[bool, List[str]]:
    """
    Função principal para executar o processo ETL.
    Retorna uma tupla (Sucesso, Lista de Logs para UI).
    """
    ui_logs = []

    def log_msg(message: str, level='info'):
        """Helper interno para logar no console e guardar para a UI"""
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

        # 1. Conexões com bancos de dados
        # A classe DatabaseConnector espera o NOME da variável de ambiente no .env
        
        # Conexão Origem (WordPress)
        engine_wp = DatabaseConnector.get_engine(
            "mysql+pymysql", "WP_USER", "WP_PASS", "WP_HOST", "WP_PORT", "WP_DB_NAME"
        )
        
        # Conexão Destino (Data Warehouse)
        # Hack: Definimos variáveis de ambiente "virtuais" caso elas não estejam no .env 
        # para manter o padrão da classe DatabaseConnector, ou garantimos que elas existam no .env.
        if not os.getenv("POSTGRES_HOST"): os.environ["POSTGRES_HOST"] = "dw"
        if not os.getenv("POSTGRES_PORT"): os.environ["POSTGRES_PORT"] = "5432"

        engine_dw = DatabaseConnector.get_engine(
            "postgresql", 
            "POSTGRES_USER", 
            "POSTGRES_PASSWORD", 
            "POSTGRES_HOST",  # Passando o nome da chave, não o valor "dw"
            "POSTGRES_PORT",  # Passando o nome da chave, não o valor "5432"
            "POSTGRES_DB"
        )

        # 2. Pipeline Principal
        log_msg("📥 Extraindo dados consolidados (Pedidos + Clientes + Financeiro)...")
        df_raw = extract_wordpress_orders(engine_wp)

        if not df_raw.empty:
            log_msg(f"🛠️ {len(df_raw)} linhas extraídas. Iniciando tratamento...")

            df_clean = transform_orders(df_raw)
            
            log_msg("💾 Salvando dados no Data Warehouse...")
            
            # Utiliza chunksize para evitar estouro de memória em grandes volumes
            df_clean.to_sql('pedidos_consolidados', engine_dw, if_exists='replace', index=False, chunksize=1000)

            log_msg(f"Carga concluída! {len(df_clean)} registros atualizados.", level='success')

        else:
            log_msg("Nenhum pedido encontrado na tabela wp_wc_orders.", level="info")

        return True, ui_logs

    except Exception as e:
        log_msg(f"Falha Crítica no ETL: {str(e)}", "error")
        return False, ui_logs