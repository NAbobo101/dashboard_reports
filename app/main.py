"""
Streamlit Data Browser para MySQL.

- Navega por schemas permitidos e exibe tabelas/views com paginação.
- Segurança: schema/tabela são validados (whitelist + regex) antes de interpolar SQL.
- Conexão: preferir credenciais read-only (STREAMLIT_RO_USER/STREAMLIT_RO_PASSWORD).

Env:
  DB_HOST, DB_PORT, DB_NAME (opcional; default=information_schema)
  STREAMLIT_RO_USER, STREAMLIT_RO_PASSWORD (preferencial)
  DB_USER, DB_PASSWORD (fallback)
"""


import os
import re
from dataclasses import dataclass
from typing import List, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


# ------------------------------------------------------------------------------
# Config / Setup (UI)
# ------------------------------------------------------------------------------

# Config global do Streamlit. Evite recomputar/alterar isso em runtime.
st.set_page_config(
    page_title="Relatórios Stellar Beauty",
    page_icon="📄",
    layout="wide",
)


@dataclass(frozen=True)
class DBConfig:
    """
    Estrutura imutável para agrupar parâmetros de conexão.

    Nota:
      Em MySQL a URL do driver exige um database no path.
      Usar information_schema como default é robusto e evita falhas quando DB_NAME
      não existe (ou quando o usuário não tem permissão no DB_NAME).
    """
    host: str
    port: int
    user: str
    password: str
    default_database: str = "information_schema"


def _required_env(name: str) -> str:
    """
    Helper: lê uma variável de ambiente obrigatória.

    Observação:
      Atualmente não é usada no fluxo principal, mas é útil para evoluções onde
      você prefira falhar cedo quando uma env for mandatória.
    """
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


def load_db_config() -> DBConfig:
    """
    Lê variáveis de ambiente e monta a configuração de conexão.

    Estratégia:
      1) Preferimos credenciais do usuário read-only (STREAMLIT_RO_*).
      2) Se não existir, caímos para DB_USER/DB_PASSWORD (útil em dev local).

    Motivo:
      O Streamlit é uma camada de leitura/consulta. Rodar com permissões mínimas
      reduz risco de dano acidental (DROP/UPDATE) e limita impacto de incidentes.
    """
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "3306"))

    # Preferência: usuário dedicado read-only para o Streamlit.
    user = os.getenv("STREAMLIT_RO_USER") or os.getenv("DB_USER")
    password = os.getenv("STREAMLIT_RO_PASSWORD") or os.getenv("DB_PASSWORD")

    # Falha explícita evita URLs inválidas do tipo mysql://None:None@...
    if not user or not password:
        raise RuntimeError(
            "Credenciais do banco não configuradas. Defina STREAMLIT_RO_USER/STREAMLIT_RO_PASSWORD "
            "ou DB_USER/DB_PASSWORD no .env."
        )

    # DB_NAME é opcional. Se existir e for válido, usamos; senão, fallback seguro.
    default_db = os.getenv("DB_NAME", "information_schema")

    # _is_safe_identifier é definido abaixo; ok em Python porque a função só é
    # avaliada quando load_db_config() roda (depois do módulo carregado).
    if not _is_safe_identifier(default_db):
        default_db = "information_schema"

    return DBConfig(
        host=host,
        port=port,
        user=user,
        password=password,
        default_database=default_db,
    )


@st.cache_resource(show_spinner=False)
def get_engine(cfg: DBConfig) -> Engine:
    """
    Cria e mantém um SQLAlchemy Engine em cache.

    Boas práticas:
      - cache_resource: evita reconectar a cada rerun (Streamlit reexecuta script)
      - pool_pre_ping: detecta conexões mortas no pool e reconecta
      - pool_recycle: evita timeout em alguns proxies/infra (ex.: 30m)
    """
    # Observação: charset utf8mb4 garante suporte completo (acentos, emojis).
    url = (
        f"mysql+pymysql://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.default_database}"
        f"?charset=utf8mb4"
    )

    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,         # pool pequeno é suficiente para Streamlit
        max_overflow=10,     # permite bursts curtos
        pool_recycle=1800,   # recicla conexões antigas (em segundos)
    )
    return engine


# ------------------------------------------------------------------------------
# Helpers de metadados e segurança
# ------------------------------------------------------------------------------

# Whitelist de schemas disponíveis na UI.
# Importante:
# - Reduz superfície: o usuário só navega em databases conhecidos e esperados.
# - Evita ataques/bugs via namespacing (schema vindo do input).
ALLOWED_SCHEMAS = ("staging", "core", "wordpress", "active_campaign")

# Regex conservadora: só permite letras/números/underscore.
# Isso bloqueia espaços, hífen, ponto, aspas, etc.
# (MySQL até permite nomes com outros chars via `backticks`, mas aqui preferimos
#  ser super restritivos por segurança.)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _is_safe_identifier(value: str) -> bool:
    """
    Valida identificadores SQL (schema/tabela/view) de forma conservadora.

    Por que isso importa:
      - Em SQLAlchemy, schema/tabela não podem ser bind parameters.
      - Se interpolarmos string sem validação, abrimos brecha para SQL injection.

    Regra:
      Permite apenas [A-Za-z0-9_]+.
    """
    return bool(value) and bool(_IDENTIFIER_RE.match(value))


def is_safe_identifier(value: str) -> bool:
    """
    Compatibilidade: mantém o nome original usado no restante do código.

    Nota:
      Idealmente use apenas uma função, mas manter esse wrapper evita retrabalho
      e deixa refactors futuros menos invasivos.
    """
    return _is_safe_identifier(value)


def list_tables_and_views(engine: Engine, schema: str) -> List[Tuple[str, str]]:
    """
    Lista tabelas e views do schema usando information_schema.

    Retorno:
      Lista de (TABLE_NAME, TABLE_TYPE), onde TABLE_TYPE é:
        - 'BASE TABLE'
        - 'VIEW'

    Observação de permissão:
      O usuário precisa ter permissão para "enxergar" os objetos do schema alvo.
      Se ele não tiver SELECT/SHOW VIEW, é comum retornar lista vazia.
    """
    q = text("""
        SELECT TABLE_NAME, TABLE_TYPE
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = :schema
        ORDER BY TABLE_TYPE, TABLE_NAME
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"schema": schema}).fetchall()

    # Convertemos para uma estrutura simples para facilitar uso na UI.
    return [(r[0], r[1]) for r in rows]


def fetch_page(engine: Engine, schema: str, table: str, limit: int, offset: int) -> pd.DataFrame:
    """
    Busca uma página de dados de uma tabela/view com LIMIT/OFFSET.

    Segurança:
      - schema vem de ALLOWED_SCHEMAS (whitelist)
      - table é validada por regex
      - limit/offset são bind params (seguros)

    Nota de performance:
      - OFFSET grande pode ser lento em tabelas grandes.
      - Para relatórios, crie views/tabelas no schema "limpo" com índices adequados.
    """
    if schema not in ALLOWED_SCHEMAS:
        raise ValueError("Schema inválido.")

    if not is_safe_identifier(table):
        raise ValueError("Nome de tabela/view inválido.")

    # Limites conservadores para evitar travar a UI ou explodir memória.
    if limit < 1 or limit > 5000:
        raise ValueError("Limit fora do intervalo permitido.")

    if offset < 0:
        raise ValueError("Offset inválido.")

    # Interpolação controlada: `schema` e `table` já foram validados.
    sql = text(f"SELECT * FROM `{schema}`.`{table}` LIMIT :limit OFFSET :offset")

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"limit": limit, "offset": offset})

    return df


def get_server_info(engine: Engine) -> dict:
    """
    Coleta informações básicas do servidor.

    Útil para:
      - confirmar que a conexão funciona
      - confirmar qual usuário MySQL está autenticado (CURRENT_USER)
      - validar clock do servidor (NOW)
    """
    info: dict = {}
    with engine.connect() as conn:
        info["version"] = conn.execute(text("SELECT VERSION()")).scalar()
        info["now"] = conn.execute(text("SELECT NOW(6)")).scalar()
        info["current_user"] = conn.execute(text("SELECT CURRENT_USER()")).scalar()
    return info


def get_visible_databases(engine: Engine) -> List[str]:
    """
    Retorna databases visíveis via SHOW DATABASES.

    Por que isso é valioso:
      Quando o usuário reclama "não aparece tabela", quase sempre é permissão.
      Se o schema nem aparece aqui, o problema é GRANT faltando.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("SHOW DATABASES")).fetchall()
    return [r[0] for r in rows]


# ------------------------------------------------------------------------------
# UI
# ------------------------------------------------------------------------------

def render_sidebar() -> dict:
    """
    Renderiza a sidebar e retorna parâmetros de navegação.

    Observação:
      Streamlit reexecuta o script; essa função deve ser "barata".
    """
    st.sidebar.title("⚙️ Configurações")

    # Mantém default em wordpress, sem depender de índice fixo (mais resiliente).
    default_schema = "wordpress"
    default_index = ALLOWED_SCHEMAS.index(default_schema) if default_schema in ALLOWED_SCHEMAS else 0

    schema = st.sidebar.selectbox("Schema (database)", ALLOWED_SCHEMAS, index=default_index)

    # Página pequena por padrão para evitar travar UI ao abrir tabelas grandes.
    page_size = st.sidebar.selectbox(
        "Linhas por página",
        options=[25, 50, 100, 250, 500, 1000],
        index=1
    )

    # Página é 1-based no UI; offset será calculado no main().
    page = st.sidebar.number_input("Página", min_value=1, value=1, step=1)

    return {"schema": schema, "page_size": int(page_size), "page": int(page)}


def main() -> None:
    """
    Função principal: monta UI, conecta no DB e faz browsing/paginação.

    Organização:
      - Carrega cfg e engine
      - Define abas: Browser e Saúde
      - Browser: lista tabelas/views e mostra preview paginado
      - Saúde: mostra info do servidor e diagnóstico de permissões
    """
    st.title("📊 Relatórios Stellar Beauty")
    st.caption("Navegue pelos schemas staging/core/wordpress/active_campaign e visualize dados com paginação.")

    # Falhar cedo com mensagem amigável para o operador.
    try:
        cfg = load_db_config()
    except RuntimeError as e:
        st.error("Configuração de conexão inválida.")
        st.exception(e)
        st.stop()

    # Engine cacheado (bom para UX e carga no DB).
    engine = get_engine(cfg)

    # Layout principal em abas (evita uma página longa e mistura de contexto).
    tab_browser, tab_health = st.tabs(["🔎 Data Browser", "🩺 Saúde"])

    # Sidebar controls compartilhados
    controls = render_sidebar()

    # -------------------------
    # TAB: Browser (navegação)
    # -------------------------
    with tab_browser:
        st.subheader("Explorar tabelas e views")

        schema = controls["schema"]

        # 1) Lista objetos (tabelas/views) do schema selecionado
        try:
            objects = list_tables_and_views(engine, schema)
        except SQLAlchemyError as e:
            # Erros comuns aqui: permissão/credenciais erradas/conexão com DB falhando.
            st.error("Falha ao listar tabelas/views. Verifique conexão e permissões.")
            st.exception(e)
            st.stop()

        # Se não há objetos visíveis, pode ser schema vazio OU falta de permissão.
        if not objects:
            st.warning(
                f"Nenhuma tabela/view encontrada em `{schema}`.\n\n"
                "Isso normalmente é **permissão** (o usuário não tem SELECT/SHOW VIEW nesse schema) "
                "ou o schema não possui objetos.\n"
                "Confira a aba **Saúde** para ver quais databases o usuário enxerga."
            )
            st.stop()

        # Prepara labels amigáveis (nome + tipo)
        labels = [f"{name} ({typ})" for name, typ in objects]

        # selectbox com índice evita problemas quando houver nomes repetidos / ordenação
        selected_idx = st.selectbox(
            "Selecione uma tabela/view",
            range(len(objects)),
            format_func=lambda i: labels[i],
        )
        table_name, table_type = objects[selected_idx]

        # Mostra contexto (schema, objeto, tipo)
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            st.write(f"**Schema:** `{schema}`")
        with col2:
            st.write(f"**Objeto:** `{table_name}`")
        with col3:
            st.write(f"**Tipo:** `{table_type}`")

        # 2) Paginação
        limit = controls["page_size"]
        page = controls["page"]
        offset = (page - 1) * limit

        st.divider()

        # 3) Carrega preview paginado
        try:
            df = fetch_page(engine, schema, table_name, limit=limit, offset=offset)
        except Exception as e:
            # Pode falhar por:
            # - falta de SELECT na tabela
            # - view que referencia objetos sem permissão
            # - table dropada enquanto a UI estava aberta
            st.error("Falha ao buscar dados. Verifique se a tabela existe e se você tem permissão.")
            st.exception(e)
            st.stop()

        st.write(f"Mostrando **{len(df)}** linhas (page={page}, limit={limit}).")
        st.dataframe(df, use_container_width=True)

        # Exporta apenas a página atual para evitar CSVs enormes e travamentos.
        st.download_button(
            label="⬇️ Baixar esta página (CSV)",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"{schema}.{table_name}.page{page}.csv",
            mime="text/csv",
        )

    # -------------------------
    # TAB: Saúde (diagnóstico)
    # -------------------------
    with tab_health:
        st.subheader("Status da conexão e permissões")

        # 1) Informações do servidor
        try:
            info = get_server_info(engine)
        except SQLAlchemyError as e:
            st.error("Não foi possível consultar informações do servidor.")
            st.exception(e)
            st.stop()

        st.success("Conexão com MySQL OK ✅")
        st.write("**Versão:**", info.get("version"))
        st.write("**Horário do servidor:**", info.get("now"))
        st.write("**Usuário autenticado:**", info.get("current_user"))

        # 2) Diagnóstico de permissões via SHOW DATABASES
        try:
            dbs = get_visible_databases(engine)
            st.write("**Databases visíveis (SHOW DATABASES):**")
            st.code("\n".join(dbs) if dbs else "(nenhum)")

            # Alertas objetivos para os schemas importantes do produto
            # (não bloqueia a UI, mas direciona a correção para grants).
            missing = [s for s in ("wordpress", "active_campaign") if s not in dbs]
            if missing:
                st.warning(
                    "O usuário atual **não enxerga** os databases abaixo:\n"
                    + "\n".join([f"- `{s}`" for s in missing])
                    + "\n\nIsso confirma que falta GRANT (SELECT/SHOW VIEW em <db>.*) para esse usuário."
                )
        except SQLAlchemyError as e:
            st.error("Falha ao executar SHOW DATABASES (diagnóstico de permissões).")
            st.exception(e)

        st.caption("No Adminer, conecte com Server=db e as credenciais do .env.")


# Padrão Python: executa apenas quando rodado como script.
# Em Streamlit, o arquivo é executado como módulo, mas manter isso é ok e deixa
# o entrypoint explícito.
if __name__ == "__main__":
    main()
