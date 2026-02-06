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
# Config / Setup
# ------------------------------------------------------------------------------

st.set_page_config(
    page_title="Relatórios Stellar Beauty",
    page_icon="📄",
    layout="wide",
)


@dataclass(frozen=True)
class DBConfig:
    """Configuração de conexão lida do ambiente."""
    host: str
    port: int
    user: str
    password: str

    # Em MySQL, um database é exigido na URL.
    # Usar information_schema como default é mais robusto (sempre existe).
    default_database: str = "information_schema"


def _required_env(name: str) -> str:
    """
    Lê uma variável de ambiente obrigatória.
    Se não existir, levanta erro claro (para aparecer no Streamlit).
    """
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


def load_db_config() -> DBConfig:
    """
    Lê variáveis de ambiente.
    Prioriza credenciais read-only do Streamlit:
      STREAMLIT_RO_USER / STREAMLIT_RO_PASSWORD
    e faz fallback para:
      DB_USER / DB_PASSWORD

    Isso garante que o Streamlit rode com o usuário correto (least-privilege).
    """
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "3306"))

    # 1) Prioridade: usuário read-only do Streamlit (recomendado)
    user = os.getenv("STREAMLIT_RO_USER") or os.getenv("DB_USER")
    password = os.getenv("STREAMLIT_RO_PASSWORD") or os.getenv("DB_PASSWORD")

    if not user or not password:
        # Mensagem direta pra evitar URL inválida (None/None)
        raise RuntimeError(
            "Credenciais do banco não configuradas. Defina STREAMLIT_RO_USER/STREAMLIT_RO_PASSWORD "
            "ou DB_USER/DB_PASSWORD no .env."
        )

    # DB_NAME é opcional; se vier, tudo bem. Mas o default mais robusto é information_schema.
    default_db = os.getenv("DB_NAME", "information_schema")

    # Se DB_NAME vier com algo inexistente (ex.: warehouse_db), isso pode quebrar conexão.
    # Então, garantimos que o default seja seguro:
    if not _is_safe_identifier(default_db):
        default_db = "information_schema"

    return DBConfig(host=host, port=port, user=user, password=password, default_database=default_db)


@st.cache_resource(show_spinner=False)
def get_engine(cfg: DBConfig) -> Engine:
    """
    Cria e mantém o Engine em cache (evita reconectar a cada rerun do Streamlit).
    pool_pre_ping=True: evita conexões "mortas" no pool.
    """
    # Observação: charset utf8mb4 garante suporte completo a caracteres.
    url = (
        f"mysql+pymysql://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.default_database}"
        f"?charset=utf8mb4"
    )

    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_recycle=1800,
    )
    return engine


# ------------------------------------------------------------------------------
# Helpers de metadados e segurança
# ------------------------------------------------------------------------------

ALLOWED_SCHEMAS = ("staging", "core", "reporting")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _is_safe_identifier(value: str) -> bool:
    """
    Valida identificadores SQL (schema/tabela/view) de forma conservadora.
    Permite apenas letras, números e underscore.
    """
    return bool(value) and bool(_IDENTIFIER_RE.match(value))


def is_safe_identifier(value: str) -> bool:
    """Compat: mantém o nome original usado no restante do código."""
    return _is_safe_identifier(value)


def list_tables_and_views(engine: Engine, schema: str) -> List[Tuple[str, str]]:
    """
    Lista tabelas e views de um schema usando information_schema.
    Retorna lista de tuplas: (nome, tipo) onde tipo é 'BASE TABLE' ou 'VIEW'
    """
    q = text("""
        SELECT TABLE_NAME, TABLE_TYPE
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = :schema
        ORDER BY TABLE_TYPE, TABLE_NAME
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"schema": schema}).fetchall()
    return [(r[0], r[1]) for r in rows]


def fetch_page(engine: Engine, schema: str, table: str, limit: int, offset: int) -> pd.DataFrame:
    """
    Busca uma página de dados com LIMIT/OFFSET.
    Como schema/tabela não podem ser bind parameters, validamos e interpolamos com segurança.
    """
    if schema not in ALLOWED_SCHEMAS:
        raise ValueError("Schema inválido.")

    if not is_safe_identifier(table):
        raise ValueError("Nome de tabela/view inválido.")

    if limit < 1 or limit > 5000:
        raise ValueError("Limit fora do intervalo permitido.")

    if offset < 0:
        raise ValueError("Offset inválido.")

    sql = text(f"SELECT * FROM `{schema}`.`{table}` LIMIT :limit OFFSET :offset")
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"limit": limit, "offset": offset})
    return df


def get_server_info(engine: Engine) -> dict:
    """Coleta informações básicas do servidor para a aba de saúde."""
    info = {}
    with engine.connect() as conn:
        info["version"] = conn.execute(text("SELECT VERSION()")).scalar()
        info["now"] = conn.execute(text("SELECT NOW(6)")).scalar()
        info["current_user"] = conn.execute(text("SELECT CURRENT_USER()")).scalar()
    return info


def get_visible_databases(engine: Engine) -> List[str]:
    """
    Retorna databases visíveis para o usuário atual.
    Diagnóstico rápido de permissão: se não aparece reporting aqui, é GRANT faltando.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("SHOW DATABASES")).fetchall()
    return [r[0] for r in rows]


# ------------------------------------------------------------------------------
# UI
# ------------------------------------------------------------------------------

def render_sidebar() -> dict:
    """Sidebar com opções globais."""
    st.sidebar.title("⚙️ Configurações")

    schema = st.sidebar.selectbox("Schema (database)", ALLOWED_SCHEMAS, index=1)

    page_size = st.sidebar.selectbox(
        "Linhas por página",
        options=[25, 50, 100, 250, 500, 1000],
        index=1
    )

    page = st.sidebar.number_input("Página", min_value=1, value=1, step=1)

    return {"schema": schema, "page_size": int(page_size), "page": int(page)}


def main() -> None:
    st.title("📊 Relatórios Stellar Beauty")
    st.caption("Navegue pelos schemas staging/core/reporting e visualize dados com paginação.")

    try:
        cfg = load_db_config()
    except RuntimeError as e:
        st.error("Configuração de conexão inválida.")
        st.exception(e)
        st.stop()

    engine = get_engine(cfg)

    tab_browser, tab_health = st.tabs(["🔎 Data Browser", "🩺 Saúde"])
    controls = render_sidebar()

    # -------------------------
    # TAB: Browser
    # -------------------------
    with tab_browser:
        st.subheader("Explorar tabelas e views")

        schema = controls["schema"]

        try:
            objects = list_tables_and_views(engine, schema)
        except SQLAlchemyError as e:
            st.error("Falha ao listar tabelas/views. Verifique conexão e permissões.")
            st.exception(e)
            st.stop()

        if not objects:
            st.warning(
                f"Nenhuma tabela/view encontrada em `{schema}`.\n\n"
                "Isso normalmente é **permissão** (o usuário não tem SELECT/SHOW VIEW nesse schema) "
                "ou o schema não possui objetos.\n"
                "Confira a aba **Saúde** para ver quais databases o usuário enxerga."
            )
            st.stop()

        labels = [f"{name} ({typ})" for name, typ in objects]
        selected_idx = st.selectbox("Selecione uma tabela/view", range(len(objects)), format_func=lambda i: labels[i])
        table_name, table_type = objects[selected_idx]

        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            st.write(f"**Schema:** `{schema}`")
        with col2:
            st.write(f"**Objeto:** `{table_name}`")
        with col3:
            st.write(f"**Tipo:** `{table_type}`")

        limit = controls["page_size"]
        page = controls["page"]
        offset = (page - 1) * limit

        st.divider()

        try:
            df = fetch_page(engine, schema, table_name, limit=limit, offset=offset)
        except Exception as e:
            st.error("Falha ao buscar dados. Verifique se a tabela existe e se você tem permissão.")
            st.exception(e)
            st.stop()

        st.write(f"Mostrando **{len(df)}** linhas (page={page}, limit={limit}).")
        st.dataframe(df, use_container_width=True)

        st.download_button(
            label="⬇️ Baixar esta página (CSV)",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"{schema}.{table_name}.page{page}.csv",
            mime="text/csv",
        )

    # -------------------------
    # TAB: Saúde
    # -------------------------
    with tab_health:
        st.subheader("Status da conexão e permissões")

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

        # Diagnóstico de permissão: databases visíveis
        try:
            dbs = get_visible_databases(engine)
            st.write("**Databases visíveis (SHOW DATABASES):**")
            st.code("\n".join(dbs) if dbs else "(nenhum)")

            # Alerta direto para o caso do reporting
            if "reporting" not in dbs:
                st.warning(
                    "O usuário atual **não enxerga** o database `reporting`.\n"
                    "Isso confirma que falta GRANT para esse usuário (SELECT/SHOW VIEW em reporting.*)."
                )
        except SQLAlchemyError as e:
            st.error("Falha ao executar SHOW DATABASES (diagnóstico de permissões).")
            st.exception(e)

        st.caption("No Adminer, conecte com Server=db e as credenciais do .env.")


if __name__ == "__main__":
    main()
