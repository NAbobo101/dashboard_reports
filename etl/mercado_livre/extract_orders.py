import json
import os
import time
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import (
    # NOVO: serviço interno OAuth
    MELI_OAUTH_SERVICE_URL,
    MELI_INTERNAL_KEY,
    # Fallback legado (se existir)
    TOKEN_BROKER_URL,
    TOKEN_BROKER_KEY,
    # Seller
    MELI_SELLER_ID,
    # DB
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASS,
    MYSQL_DB,
)

# -----------------------------
# Configurações do extrator
# -----------------------------
API_BASE = "https://api.mercadolibre.com"
ORDERS_SEARCH = f"{API_BASE}/orders/search"
ORDER_DETAIL = f"{API_BASE}/orders/{{order_id}}"
USERS_ME = f"{API_BASE}/users/me"

FETCH_DETAIL_WHEN_MISSING_ITEMS = True
PAGE_LIMIT = 50
COMMIT_EVERY = 100
TOKEN_SKEW_SECONDS = 60

# NOVO: fallback quando Orders API estiver bloqueada por PolicyAgent (403)
# (roadmap atualizado: relatório de vendas via Billing Integration)
ENABLE_BILLING_FALLBACK = os.environ.get("MELI_ENABLE_BILLING_FALLBACK", "1").strip() not in ("0", "false", "False")

# -----------------------------
# SQL (UPSERT)
# -----------------------------
# Alinhado com 060_create_mercado_livre_core.sql:
# meli_sellers(seller_id, nickname, site_id, email, raw_json, updated_at)
SELLER_UPSERT = """
INSERT INTO meli_sellers (seller_id, nickname, site_id, email, raw_json)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  nickname=VALUES(nickname),
  site_id=VALUES(site_id),
  email=VALUES(email),
  raw_json=VALUES(raw_json);
"""

ORDER_UPSERT = """
INSERT INTO meli_orders (
  order_id, seller_id, pack_id, buyer_id, shipping_id,
  status, status_detail, order_type,
  currency_id, total_amount, paid_amount,
  date_created, date_closed, last_updated,
  raw_json
) VALUES (
  %s,%s,%s,%s,%s,
  %s,%s,%s,
  %s,%s,%s,
  %s,%s,%s,
  %s
)
ON DUPLICATE KEY UPDATE
  status=VALUES(status),
  status_detail=VALUES(status_detail),
  order_type=VALUES(order_type),
  currency_id=VALUES(currency_id),
  total_amount=VALUES(total_amount),
  paid_amount=VALUES(paid_amount),
  date_created=VALUES(date_created),
  date_closed=VALUES(date_closed),
  last_updated=VALUES(last_updated),
  raw_json=VALUES(raw_json),
  synced_at=CURRENT_TIMESTAMP;
"""

ITEM_UPSERT = """
INSERT INTO meli_order_items (
  order_id, item_id, variation_id, title, sku,
  quantity, currency_id, unit_price, full_unit_price, sale_fee, raw_json
) VALUES (
  %s,%s,%s,%s,%s,
  %s,%s,%s,%s,%s,%s
)
ON DUPLICATE KEY UPDATE
  title=VALUES(title),
  sku=VALUES(sku),
  quantity=VALUES(quantity),
  currency_id=VALUES(currency_id),
  unit_price=VALUES(unit_price),
  full_unit_price=VALUES(full_unit_price),
  sale_fee=VALUES(sale_fee),
  raw_json=VALUES(raw_json),
  synced_at=CURRENT_TIMESTAMP;
"""


# -----------------------------
# Helpers
# -----------------------------
def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def _safe_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _parse_dt_to_utc_naive(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000-00:00")


def last_week_window() -> Tuple[str, str]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=7)
    return iso_utc(start), iso_utc(now)


def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _now_ts() -> int:
    return int(time.time())


def _digits_only(s: Any) -> str:
    return "".join([c for c in str(s) if c.isdigit()])


def _as_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return default
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def _token_prefix(token: Optional[str]) -> str:
    if not token:
        return ""
    return token[:12]


def _is_policyagent_unauthorized(resp: requests.Response) -> bool:
    """
    Detecta o bloqueio específico visto por vocês:
    {"status":403,"blocked_by":"PolicyAgent","code":"PA_UNAUTHORIZED_RESULT_FROM_POLICIES"...}
    """
    if resp.status_code != 403:
        return False
    try:
        data = resp.json() or {}
    except Exception:
        return False
    return (
        isinstance(data, dict)
        and data.get("blocked_by") == "PolicyAgent"
        and str(data.get("code") or "").startswith("PA_")
    )


# -----------------------------
# Token Provider (novo + fallback)
# -----------------------------
class TokenProvider:
    """
    - Preferência: serviço interno /internal/meli/token (relatorios-meli-oauth)
    - Fallback: WP broker /wp-json/meli/v1/token (legado)
    Cacheia access_token até expirar (se expires_at existir).

    Importante:
    - 400 detail.error=not_connected => precisa autorizar no WP (/wp-json/meli/v1/auth/start)
    - 409 => reauth_required (refresh inválido/expirado)
    """

    def __init__(self, session: requests.Session):
        self.session = session
        self._bundle: Dict[str, Any] = {}
        self._access_token: Optional[str] = None
        self._expires_at: int = 0
        self._seller_id: Optional[str] = None

    def invalidate(self) -> None:
        self._access_token = None
        self._expires_at = 0
        self._bundle = {}
        self._seller_id = None

    def seller_id(self) -> str:
        return str(self._seller_id or MELI_SELLER_ID)

    def get_bundle(self) -> Dict[str, Any]:
        return dict(self._bundle) if self._bundle else {}

    def get_token(self, force_refresh: bool = False) -> str:
        now = _now_ts()
        exp = self._expires_at or 0

        if force_refresh or not self._access_token or (exp and exp <= now + TOKEN_SKEW_SECONDS):
            self._fetch_bundle()

        if not self._access_token:
            raise RuntimeError("Não foi possível obter access_token.")
        return self._access_token

    def _fetch_bundle(self) -> None:
        b: Dict[str, Any] = {}

        if MELI_OAUTH_SERVICE_URL and MELI_INTERNAL_KEY:
            b = self._get_from_oauth_service()

        if not b and TOKEN_BROKER_URL and TOKEN_BROKER_KEY:
            b = self._get_from_wp_broker()

        if not b:
            raise RuntimeError(
                "Sem fonte de token disponível. Configure MELI_OAUTH_SERVICE_URL+MELI_INTERNAL_KEY "
                "ou MELI_TOKEN_BROKER_URL+MELI_TOKEN_BROKER_KEY."
            )

        self._bundle = b
        self._access_token = b.get("access_token")
        self._expires_at = _as_int(b.get("expires_at"), default=0)

        sid = b.get("seller_id") or MELI_SELLER_ID
        self._seller_id = str(sid)

        if not self._access_token:
            raise RuntimeError(f"Fonte de token retornou payload inesperado: {b}")

    def _get_from_oauth_service(self) -> Dict[str, Any]:
        url = f"{MELI_OAUTH_SERVICE_URL.rstrip('/')}/internal/meli/token"
        r = self.session.get(
            url,
            headers={"X-Internal-Key": str(MELI_INTERNAL_KEY)},
            params={"seller_id": str(MELI_SELLER_ID)},
            timeout=20,
        )

        # 400 not_connected
        if r.status_code == 400:
            try:
                data = r.json() or {}
            except Exception:
                data = {}
            detail = data.get("detail") if isinstance(data, dict) else None
            if isinstance(detail, dict) and detail.get("error") == "not_connected":
                raise RuntimeError(
                    "reauth_required: seller não conectado. Abra /wp-json/meli/v1/auth/start no WordPress e autorize."
                )

        # 409 => refresh inválido/expirado
        if r.status_code == 409:
            raise RuntimeError(
                "reauth_required: refresh inválido/expirado. Abra /wp-json/meli/v1/auth/start no WordPress e refaça."
            )

        r.raise_for_status()
        data = r.json() or {}

        if "access_token" not in data:
            raise RuntimeError(f"OAuth service retornou payload inesperado: {data}")

        return data

    def _get_from_wp_broker(self) -> Dict[str, Any]:
        r = self.session.get(
            f"{TOKEN_BROKER_URL.rstrip('/')}/wp-json/meli/v1/token",
            headers={"X-Internal-Key": str(TOKEN_BROKER_KEY)},
            params={"seller_id": str(MELI_SELLER_ID)},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json() or {}
        if "access_token" not in data:
            raise RuntimeError(f"Token broker retornou payload inesperado: {data}")
        return data


# -----------------------------
# Mercado Livre API calls
# -----------------------------
def meli_get(
    session: requests.Session,
    url: str,
    access_token: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> requests.Response:
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    return session.get(url, headers=headers, params=params, timeout=timeout)


def _should_refresh_on_status(status_code: int) -> bool:
    # 401 clássico; 403 pode vir quando token fica inválido/sem policy no edge
    return status_code in (401, 403)


def fetch_me(session: requests.Session, token_provider: TokenProvider) -> Dict[str, Any]:
    bundle = token_provider.get_bundle()
    if isinstance(bundle.get("me"), dict) and bundle["me"]:
        return bundle["me"]

    access_token = token_provider.get_token()
    r = meli_get(session, USERS_ME, access_token, timeout=20)

    if _should_refresh_on_status(r.status_code):
        access_token = token_provider.get_token(force_refresh=True)
        r = meli_get(session, USERS_ME, access_token, timeout=20)

    r.raise_for_status()
    return r.json() or {}


def fetch_orders(session: requests.Session, token_provider: TokenProvider) -> Iterable[Dict[str, Any]]:
    """
    ATENÇÃO (roadmap atualizado):
    - Em algumas contas/apps o endpoint /orders/search pode retornar 403 PolicyAgent,
      mesmo com vários scopes.
    - Nesse caso, esse extrator não consegue prosseguir via Orders API.
      O caminho recomendado passa a ser o extract_sales_report.py (Billing Integration).
    """
    date_from, date_to = last_week_window()
    limit = PAGE_LIMIT
    offset = 0

    while True:
        params = {
            "seller": _digits_only(token_provider.seller_id()),
            "order.date_created.from": date_from,
            "order.date_created.to": date_to,
            "limit": limit,
            "offset": offset,
        }

        access_token = token_provider.get_token()
        r = meli_get(session, ORDERS_SEARCH, access_token, params=params, timeout=35)

        # tenta 1 refresh se 401/403 genérico
        if _should_refresh_on_status(r.status_code):
            access_token = token_provider.get_token(force_refresh=True)
            r = meli_get(session, ORDERS_SEARCH, access_token, params=params, timeout=35)

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "2") or "2")
            time.sleep(max(wait, 1))
            continue

        # Se for PolicyAgent, falha com mensagem clara
        if _is_policyagent_unauthorized(r):
            body_prefix = r.text[:500]
            token_pref = _token_prefix(access_token)
            msg = (
                "orders_api_blocked_by_policyagent: /orders/search retornou 403 PolicyAgent.\n"
                f"- token_prefix={token_pref}\n"
                f"- seller={params.get('seller')}\n"
                f"- body={body_prefix}\n"
                "Ajuste recomendado no roadmap: usar Billing Integration (extract_sales_report.py) "
                "para obter relatório de vendas."
            )
            if ENABLE_BILLING_FALLBACK:
                # Para não mascarar o problema: levantamos erro explícito (o orquestrador decide chamar o outro job)
                raise RuntimeError(msg)
            raise RuntimeError(msg)

        if r.status_code == 403:
            # outros 403: log curto e deixa raise_for_status detonar
            print("403 body=", r.text[:500])

        r.raise_for_status()
        data = r.json() or {}

        results = data.get("results", []) or []
        if not results:
            break

        for o in results:
            yield o

        paging = data.get("paging", {}) or {}
        total = int(paging.get("total", 0) or 0)
        offset += limit
        if offset >= total:
            break


def fetch_order_detail(
    session: requests.Session, token_provider: TokenProvider, order_id: Any
) -> Optional[Dict[str, Any]]:
    url = ORDER_DETAIL.format(order_id=order_id)

    access_token = token_provider.get_token()
    r = meli_get(session, url, access_token, timeout=35)

    if _should_refresh_on_status(r.status_code):
        access_token = token_provider.get_token(force_refresh=True)
        r = meli_get(session, url, access_token, timeout=35)

    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", "2") or "2")
        time.sleep(max(wait, 1))
        access_token = token_provider.get_token()
        r = meli_get(session, url, access_token, timeout=35)

    if r.status_code == 404:
        return None

    if _is_policyagent_unauthorized(r):
        body_prefix = r.text[:500]
        raise RuntimeError(
            "orders_api_blocked_by_policyagent: /orders/{id} retornou 403 PolicyAgent. "
            f"order_id={order_id} body={body_prefix}"
        )

    r.raise_for_status()
    return r.json()


# -----------------------------
# MySQL (driver)
# -----------------------------
def mysql_connect():
    try:
        import MySQLdb  # type: ignore

        conn = MySQLdb.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            passwd=MYSQL_PASS,
            db=MYSQL_DB,
            charset="utf8mb4",
            autocommit=False,
        )
        return conn
    except Exception:
        import pymysql  # type: ignore

        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            database=MYSQL_DB,
            charset="utf8mb4",
            autocommit=False,
        )
        return conn


# -----------------------------
# Transform (Order -> rows)
# -----------------------------
def extract_order_row(order: Dict[str, Any], seller_id: int) -> Tuple:
    order_id_raw = order.get("id")
    if order_id_raw is None:
        raise ValueError("order sem id")
    order_id = int(order_id_raw)

    pack_id = order.get("pack_id") or order.get("packId")
    pack_id = int(pack_id) if pack_id is not None else None

    buyer_id = None
    if isinstance(order.get("buyer"), dict) and order["buyer"].get("id") is not None:
        buyer_id = int(order["buyer"]["id"])

    shipping_id = None
    if isinstance(order.get("shipping"), dict) and order["shipping"].get("id") is not None:
        shipping_id = int(order["shipping"]["id"])

    status = order.get("status")
    status_detail = order.get("status_detail")
    order_type = order.get("order_type")

    currency_id = order.get("currency_id")
    total_amount = _safe_decimal(order.get("total_amount"))
    paid_amount = _safe_decimal(order.get("paid_amount"))

    date_created = _parse_dt_to_utc_naive(order.get("date_created")) or datetime.utcnow()
    date_closed = _parse_dt_to_utc_naive(order.get("date_closed"))
    last_updated = _parse_dt_to_utc_naive(order.get("last_updated"))

    raw_json = _safe_json(order)

    return (
        order_id,
        seller_id,
        pack_id,
        buyer_id,
        shipping_id,
        status,
        status_detail,
        order_type,
        currency_id,
        total_amount,
        paid_amount,
        date_created,
        date_closed,
        last_updated,
        raw_json,
    )


def extract_item_rows(order_id: int, order: Dict[str, Any]) -> List[Tuple]:
    items = order.get("order_items") or []
    if not isinstance(items, list):
        items = []

    rows: List[Tuple] = []
    for oi in items:
        if not isinstance(oi, dict):
            continue

        item = oi.get("item") or {}
        if not isinstance(item, dict):
            item = {}

        item_id = item.get("id") or oi.get("item_id")
        if not item_id:
            continue
        item_id = str(item_id)

        variation_id = item.get("variation_id") or oi.get("variation_id")
        variation_id = int(variation_id) if variation_id is not None else None

        title = item.get("title") or oi.get("title")
        sku = item.get("seller_sku") or item.get("sku") or oi.get("seller_sku") or oi.get("sku")

        quantity = oi.get("quantity")
        try:
            quantity = int(quantity) if quantity is not None else 0
        except Exception:
            quantity = 0

        currency_id = oi.get("currency_id") or order.get("currency_id")
        unit_price = _safe_decimal(oi.get("unit_price"))
        full_unit_price = _safe_decimal(oi.get("full_unit_price"))
        sale_fee = _safe_decimal(oi.get("sale_fee"))

        raw_json = _safe_json(oi)

        rows.append(
            (
                order_id,
                item_id,
                variation_id,
                title,
                sku,
                quantity,
                currency_id,
                unit_price,
                full_unit_price,
                sale_fee,
                raw_json,
            )
        )

    return rows


# -----------------------------
# Main ETL
# -----------------------------
def run():
    session = build_session()
    token_provider = TokenProvider(session)

    # valida que token existe (e falha com mensagem boa se não conectado)
    token_provider.get_token()

    seller_id_str = str(token_provider.seller_id() or MELI_SELLER_ID)
    seller_id_int = int(_digits_only(seller_id_str) or 0)
    if seller_id_int <= 0:
        raise RuntimeError(f"seller_id inválido: {seller_id_str}")

    me = fetch_me(session, token_provider) or {}
    seller_row = (
        seller_id_int,
        me.get("nickname"),
        me.get("site_id"),
        me.get("email"),
        _safe_json(me) if me else None,
    )

    conn = mysql_connect()
    cur = conn.cursor()

    try:
        cur.execute(SELLER_UPSERT, seller_row)

        processed = 0
        orders_batch: List[Tuple] = []
        items_batch: List[Tuple] = []

        for order in fetch_orders(session, token_provider):
            try:
                order_row = extract_order_row(order, seller_id_int)
            except Exception:
                continue

            orders_batch.append(order_row)

            order_for_items = order
            has_items = bool(order.get("order_items"))

            if (not has_items) and FETCH_DETAIL_WHEN_MISSING_ITEMS:
                detail = fetch_order_detail(session, token_provider, order.get("id"))
                if detail and isinstance(detail, dict):
                    order_for_items = detail

            try:
                order_id_int = int(order.get("id"))
            except Exception:
                continue

            items_batch.extend(extract_item_rows(order_id_int, order_for_items))

            processed += 1

            if processed % COMMIT_EVERY == 0:
                if orders_batch:
                    cur.executemany(ORDER_UPSERT, orders_batch)
                if items_batch:
                    cur.executemany(ITEM_UPSERT, items_batch)
                conn.commit()
                orders_batch.clear()
                items_batch.clear()

        if orders_batch:
            cur.executemany(ORDER_UPSERT, orders_batch)
        if items_batch:
            cur.executemany(ITEM_UPSERT, items_batch)
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    run()
