import json
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASS,
    MYSQL_DB,
)

# -----------------------------
# SQL (UPSERTs)
# -----------------------------
SELLER_UPSERT = """
INSERT INTO meli_sellers (
  seller_id, nickname, site_id, email, first_name, last_name, country_id, raw_json
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  nickname=VALUES(nickname),
  site_id=VALUES(site_id),
  email=VALUES(email),
  first_name=VALUES(first_name),
  last_name=VALUES(last_name),
  country_id=VALUES(country_id),
  raw_json=VALUES(raw_json);
"""

TOKEN_UPSERT = """
INSERT INTO meli_tokens (
  seller_id, access_token, refresh_token, token_type, scope,
  obtained_at, expires_at, last_refresh_at, revoked_at
) VALUES (
  %s,%s,%s,%s,%s,
  UTC_TIMESTAMP(), FROM_UNIXTIME(%s), UTC_TIMESTAMP(), NULL
)
ON DUPLICATE KEY UPDATE
  access_token=VALUES(access_token),
  refresh_token=VALUES(refresh_token),
  token_type=VALUES(token_type),
  scope=VALUES(scope),
  expires_at=VALUES(expires_at),
  last_refresh_at=UTC_TIMESTAMP(),
  revoked_at=NULL;
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
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _safe_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _parse_dt_to_utc_naive(s: Optional[str]) -> Optional[datetime]:
    """
    Converte ISO8601 (com tz) -> UTC naive (sem tzinfo) para gravar em DATETIME.
    Ex.: "2026-02-11T12:34:56.000-03:00" -> datetime UTC naive.
    """
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


def mysql_connect():
    """
    Conecta no MySQL usando mysqlclient (MySQLdb) se disponível;
    se não, tenta PyMySQL.
    """
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
        try:
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
        except Exception as e:
            raise RuntimeError(
                "Falha ao conectar no MySQL. Instale 'mysqlclient' (MySQLdb) ou 'pymysql'."
            ) from e


# -----------------------------
# Builders (payload -> rows)
# -----------------------------
def build_seller_row(seller_id: int, me: Dict[str, Any]) -> Tuple:
    return (
        seller_id,
        me.get("nickname"),
        me.get("site_id"),
        me.get("email"),
        me.get("first_name"),
        me.get("last_name"),
        me.get("country_id"),
        _safe_json(me) if me else None,
    )


def build_token_row_from_bundle(bundle: Dict[str, Any]) -> Tuple:
    """
    Espera bundle do broker:
    {
      seller_id, access_token, refresh_token, expires_at (epoch seconds),
      token_type, scope, ...
    }
    """
    seller_id = int("".join([c for c in str(bundle.get("seller_id", "")) if c.isdigit()]) or "0")
    if seller_id <= 0:
        raise ValueError(f"seller_id inválido no bundle: {bundle.get('seller_id')!r}")

    access_token = str(bundle.get("access_token") or "")
    refresh_token = str(bundle.get("refresh_token") or "")
    if not access_token or not refresh_token:
        raise ValueError("bundle sem access_token/refresh_token")

    expires_at_epoch = bundle.get("expires_at")
    try:
        expires_at_epoch = int(expires_at_epoch)
    except Exception:
        # fallback: se vier expires_in, calcula aproximado
        expires_in = int(bundle.get("expires_in") or 3600)
        expires_at_epoch = int(datetime.now(timezone.utc).timestamp()) + expires_in

    return (
        seller_id,
        access_token,
        refresh_token,
        bundle.get("token_type"),
        bundle.get("scope"),
        expires_at_epoch,
    )


def build_order_row(order: Dict[str, Any], seller_id: int) -> Tuple:
    order_id = int(order.get("id"))

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


def build_item_rows(order_id: int, order: Dict[str, Any]) -> List[Tuple]:
    items = order.get("order_items") or []
    rows: List[Tuple] = []

    for oi in items:
        item = oi.get("item") or {}
        item_id = item.get("id") or oi.get("item_id")
        if not item_id:
            continue
        item_id = str(item_id)  # item_id é alfanumérico (MLB...)

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
# Upsert API
# -----------------------------
class DbUpserter:
    """
    Uso recomendado:
      up = DbUpserter()
      up.open()
      up.upsert_seller(...)
      up.upsert_orders(...)
      up.commit()
      up.close()
    """

    def __init__(self):
        self.conn = None
        self.cur = None

    def open(self):
        if self.conn is not None:
            return
        self.conn = mysql_connect()
        self.cur = self.conn.cursor()

    def close(self):
        try:
            if self.cur:
                self.cur.close()
        except Exception:
            pass
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass
        self.cur = None
        self.conn = None

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()

    def upsert_seller(self, seller_row: Tuple):
        if not self.cur:
            raise RuntimeError("DbUpserter.open() não foi chamado.")
        self.cur.execute(SELLER_UPSERT, seller_row)

    def upsert_token(self, token_row: Tuple):
        if not self.cur:
            raise RuntimeError("DbUpserter.open() não foi chamado.")
        self.cur.execute(TOKEN_UPSERT, token_row)

    def upsert_orders(self, orders: Sequence[Tuple]):
        if not self.cur:
            raise RuntimeError("DbUpserter.open() não foi chamado.")
        if not orders:
            return
        self.cur.executemany(ORDER_UPSERT, orders)

    def upsert_items(self, items: Sequence[Tuple]):
        if not self.cur:
            raise RuntimeError("DbUpserter.open() não foi chamado.")
        if not items:
            return
        self.cur.executemany(ITEM_UPSERT, items)


def chunked(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]
