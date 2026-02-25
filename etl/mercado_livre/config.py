import os
from typing import Optional


def _get_required_env(name: str) -> str:
    val = os.environ.get(name)
    if val is None or str(val).strip() == "":
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return str(val).strip()


def _get_optional_env(name: str, default: str = "") -> str:
    val = os.environ.get(name)
    if val is None:
        return default
    return str(val).strip()


def _get_first_env(*names: str, default: Optional[str] = None) -> str:
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    if default is None:
        raise RuntimeError(f"Nenhuma das variáveis foi definida: {', '.join(names)}")
    return default


def _is_http_url(url: str) -> bool:
    return isinstance(url, str) and (url.startswith("http://") or url.startswith("https://"))


# -----------------------------
# Mercado Livre (modo novo)
# -----------------------------
# Serviço interno OAuth (relatorios-meli-oauth) - recomendado
MELI_OAUTH_SERVICE_URL = _get_optional_env("MELI_OAUTH_SERVICE_URL", "").rstrip("/")
# chave compartilhada (WP <-> relatorios service) no modo novo
MELI_INTERNAL_KEY = _get_optional_env("MELI_INTERNAL_KEY", "")

# Se for 1 conta, fixo. Para multi-conta, seu scheduler pode variar esse env por execução.
_raw_seller = _get_required_env("MELI_SELLER_ID")
MELI_SELLER_ID = "".join([c for c in _raw_seller if c.isdigit()])
if not MELI_SELLER_ID:
    raise RuntimeError(f"MELI_SELLER_ID inválido (sem dígitos): {_raw_seller!r}")

# -----------------------------
# Mercado Livre - Billing / Reports (novo roadmap)
# -----------------------------
# Billing Integration exige "group" no endpoint /periods e também no POST de reports.
# Valores comuns:
#   group: "ML"
#   document_type: "BILL"
#   report_format: "CSV" | "XLSX" (dependendo do que a API aceitar)
MELI_BILLING_GROUP = _get_optional_env("MELI_BILLING_GROUP", "ML").strip()
MELI_BILLING_DOCUMENT_TYPE = _get_optional_env("MELI_BILLING_DOCUMENT_TYPE", "BILL").strip()
MELI_BILLING_REPORT_FORMAT = _get_optional_env("MELI_BILLING_REPORT_FORMAT", "CSV").strip().upper()

# -----------------------------
# Mercado Livre (fallback legado)
# -----------------------------
# Mantemos por compatibilidade temporária com o broker antigo (WordPress /wp-json/.../token)
TOKEN_BROKER_URL = _get_optional_env("MELI_TOKEN_BROKER_URL", "").rstrip("/")
TOKEN_BROKER_KEY = _get_optional_env("MELI_TOKEN_BROKER_KEY", "")

# -----------------------------
# MySQL (internal_net)
# -----------------------------
MYSQL_HOST = _get_optional_env("MYSQL_HOST", "db")
if MYSQL_HOST == "":
    MYSQL_HOST = "db"

# Porta
try:
    MYSQL_PORT = int(_get_optional_env("MYSQL_PORT", "3306"))
except ValueError:
    raise RuntimeError(f"MYSQL_PORT inválida: {os.environ.get('MYSQL_PORT')!r}")

# Usuário/senha: aceita chaves alternativas comuns
MYSQL_USER = _get_first_env("MYSQL_USER", "ETL_USER", default=None)
MYSQL_PASS = _get_first_env("MYSQL_PASSWORD", "MYSQL_PASS", "ETL_PASSWORD", default=None)

# Database/schema: aceita nomes alternativos
MYSQL_DB = _get_first_env("MYSQL_DATABASE", "MYSQL_DB", default="mercado_livre").strip()
if MYSQL_DB == "":
    MYSQL_DB = "mercado_livre"

# -----------------------------
# Sanity checks adicionais
# -----------------------------
if MYSQL_PORT <= 0 or MYSQL_PORT > 65535:
    raise RuntimeError(f"MYSQL_PORT fora do intervalo: {MYSQL_PORT}")

# Validação do modo OAuth:
# - Se tiver MELI_OAUTH_SERVICE_URL => modo novo (exige MELI_INTERNAL_KEY)
# - Senão => fallback legado (exige MELI_TOKEN_BROKER_URL + MELI_TOKEN_BROKER_KEY)
if MELI_OAUTH_SERVICE_URL:
    if not _is_http_url(MELI_OAUTH_SERVICE_URL):
        raise RuntimeError(
            f"MELI_OAUTH_SERVICE_URL inválida (precisa começar com http/https): {MELI_OAUTH_SERVICE_URL!r}"
        )
    if not MELI_INTERNAL_KEY:
        raise RuntimeError("MELI_INTERNAL_KEY é obrigatória quando MELI_OAUTH_SERVICE_URL está definida.")
else:
    if not TOKEN_BROKER_URL or not TOKEN_BROKER_KEY:
        raise RuntimeError(
            "Configuração OAuth ausente: defina MELI_OAUTH_SERVICE_URL + MELI_INTERNAL_KEY "
            "OU (fallback) MELI_TOKEN_BROKER_URL + MELI_TOKEN_BROKER_KEY."
        )
    if not _is_http_url(TOKEN_BROKER_URL):
        raise RuntimeError(
            f"MELI_TOKEN_BROKER_URL inválida (precisa começar com http/https): {TOKEN_BROKER_URL!r}"
        )

# Validações do Billing (só garante que não está vazio; a API valida os valores possíveis)
if MELI_BILLING_GROUP == "":
    raise RuntimeError("MELI_BILLING_GROUP não pode ser vazio (ex: ML).")
if MELI_BILLING_DOCUMENT_TYPE == "":
    raise RuntimeError("MELI_BILLING_DOCUMENT_TYPE não pode ser vazio (ex: BILL).")
if MELI_BILLING_REPORT_FORMAT == "":
    raise RuntimeError("MELI_BILLING_REPORT_FORMAT não pode ser vazio (ex: CSV).")
