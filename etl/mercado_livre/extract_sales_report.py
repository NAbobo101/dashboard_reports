"""
extract_sales_report.py

Pipeline para gerar e baixar relatório de vendas via Mercado Livre Billing Integration.

Fluxo:
1) Obter access_token do serviço interno (relatorios-meli-oauth) via /internal/meli/token
2) Listar períodos:
     GET /billing/integration/periods?group=ML&document_type=BILL
   (IMPORTANTE: document_type é obrigatório — erro 422 confirma isso)
3) Selecionar período (default: mais recente) e obter "key"
4) Criar relatório:
     POST /billing/integration/periods/key/{KEY}/reports
     Body: {"group":"ML","document_type":"BILL","report_format":"CSV"}
5) Poll status até READY:
     GET /billing/integration/reports/{FILE_ID}/status?document_type=BILL
6) Baixar arquivo:
     GET /billing/integration/reports/{FILE_ID}?document_type=BILL
7) Salvar localmente (default: /tmp)

Execução:
  python -m etl.mercado_livre.extract_sales_report

Env (via config.py):
- MELI_OAUTH_SERVICE_URL
- MELI_INTERNAL_KEY
- MELI_SELLER_ID (usado só para obter token do serviço interno)
- MELI_BILLING_GROUP (default ML)
- MELI_BILLING_DOCUMENT_TYPE (default BILL)
- MELI_BILLING_REPORT_FORMAT (default CSV)
- MELI_REPORT_OUT_DIR (opcional, default /tmp)

O script NÃO imprime o token completo em logs.
"""

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import (
    MELI_OAUTH_SERVICE_URL,
    MELI_INTERNAL_KEY,
    MELI_SELLER_ID,
    MELI_BILLING_GROUP,
    MELI_BILLING_DOCUMENT_TYPE,
    MELI_BILLING_REPORT_FORMAT,
)

API_BASE = "https://api.mercadolibre.com"

PERIODS_URL = f"{API_BASE}/billing/integration/periods"
CREATE_REPORT_URL = f"{API_BASE}/billing/integration/periods/key/{{key}}/reports"
REPORT_STATUS_URL = f"{API_BASE}/billing/integration/reports/{{file_id}}/status"
REPORT_DOWNLOAD_URL = f"{API_BASE}/billing/integration/reports/{{file_id}}"

# polling
POLL_INTERVAL_SECONDS = 3
POLL_TIMEOUT_SECONDS = 600  # 10 min: relatórios podem demorar


def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _as_str(v: Any) -> str:
    return "" if v is None else str(v)


def _safe_filename(s: str) -> str:
    keep = []
    for ch in s:
        if ch.isalnum() or ch in ("-", "_", ".", "@"):
            keep.append(ch)
        else:
            keep.append("_")
    return "".join(keep)


def _token_prefix(token: str) -> str:
    return token[:12] if token else ""


def _should_refresh_on_status(status_code: int) -> bool:
    # 401 clássico; 403 pode acontecer no edge/policies quando token está inválido/expirado
    return status_code in (401, 403)


@dataclass
class TokenBundle:
    seller_id: str
    access_token: str
    expires_at: int


class TokenProvider:
    """
    Provider do novo fluxo.
    Fonte padrão: serviço interno relatorios-meli-oauth.

    Endpoint esperado:
      GET {MELI_OAUTH_SERVICE_URL}/internal/meli/token?seller_id=...
      Header: X-Internal-Key
      Retorna: {seller_id, access_token, expires_at}
    """

    def __init__(self, session: requests.Session):
        self.session = session
        self._bundle: Optional[TokenBundle] = None

    def fetch(self) -> TokenBundle:
        if not MELI_OAUTH_SERVICE_URL or not MELI_INTERNAL_KEY:
            raise RuntimeError("MELI_OAUTH_SERVICE_URL + MELI_INTERNAL_KEY são obrigatórios para o fluxo novo.")

        url = f"{MELI_OAUTH_SERVICE_URL.rstrip('/')}/internal/meli/token"
        r = self.session.get(
            url,
            params={"seller_id": str(MELI_SELLER_ID)},
            headers={"X-Internal-Key": str(MELI_INTERNAL_KEY)},
            timeout=20,
        )

        # 400 not_connected = precisa autorizar no WP
        if r.status_code == 400:
            try:
                detail = r.json()
            except Exception:
                detail = {"raw": r.text[:200]}
            raise RuntimeError(f"token_not_connected: {detail}. Rode /wp-json/meli/v1/auth/start e autorize.")

        # 409 = regra do service para reautorização (refresh inválido/expirado)
        if r.status_code == 409:
            raise RuntimeError("reauth_required: refaça a autorização via /wp-json/meli/v1/auth/start.")

        r.raise_for_status()
        data = r.json() or {}

        access = _as_str(data.get("access_token"))
        if not access:
            raise RuntimeError(f"payload inesperado do token service: {data}")

        bundle = TokenBundle(
            seller_id=_as_str(data.get("seller_id") or MELI_SELLER_ID),
            access_token=access,
            expires_at=int(data.get("expires_at") or 0),
        )
        self._bundle = bundle
        return bundle

    def get_access_token(self, force_refresh: bool = False) -> TokenBundle:
        if force_refresh or self._bundle is None:
            return self.fetch()
        return self._bundle


def _meli_get(
    session: requests.Session,
    url: str,
    token_provider: TokenProvider,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    accept: str = "application/json",
) -> requests.Response:
    """
    GET com 1 refresh automático (401/403 -> refaz token e tenta 1x).
    """
    bundle = token_provider.get_access_token()
    r = session.get(
        url,
        headers={"Authorization": f"Bearer {bundle.access_token}", "Accept": accept},
        params=params,
        timeout=timeout,
    )

    if _should_refresh_on_status(r.status_code):
        bundle = token_provider.get_access_token(force_refresh=True)
        r = session.get(
            url,
            headers={"Authorization": f"Bearer {bundle.access_token}", "Accept": accept},
            params=params,
            timeout=timeout,
        )

    return r


def _meli_post_json(
    session: requests.Session,
    url: str,
    token_provider: TokenProvider,
    payload: Dict[str, Any],
    timeout: int = 30,
) -> requests.Response:
    """
    POST JSON com 1 refresh automático (401/403 -> refaz token e tenta 1x).
    """
    bundle = token_provider.get_access_token()
    r = session.post(
        url,
        headers={
            "Authorization": f"Bearer {bundle.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=timeout,
    )

    if _should_refresh_on_status(r.status_code):
        bundle = token_provider.get_access_token(force_refresh=True)
        r = session.post(
            url,
            headers={
                "Authorization": f"Bearer {bundle.access_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout,
        )

    return r


def list_periods(session: requests.Session, tp: TokenProvider, group: str, document_type: str) -> Any:
    """
    GET /billing/integration/periods?group=ML&document_type=BILL

    Observação crítica:
    - O erro 422 mostrou que document_type é obrigatório neste endpoint.
    """
    group = (group or "").strip()
    document_type = (document_type or "").strip()

    if not group:
        raise RuntimeError("MELI_BILLING_GROUP vazio. Ex.: ML")
    if not document_type:
        raise RuntimeError("MELI_BILLING_DOCUMENT_TYPE vazio. Ex.: BILL")

    r = _meli_get(
        session,
        PERIODS_URL,
        tp,
        params={"group": group, "document_type": document_type},
        timeout=30,
        accept="application/json",
    )

    if r.status_code >= 400:
        raise RuntimeError(f"periods_failed status={r.status_code} body={r.text[:800]}")
    return r.json()


def choose_period_key(periods_payload: Any) -> str:
    """
    A API pode devolver:
    - lista direta
    - ou dict com "periods"/"results"/"data"

    Heurística: pega o período mais recente (último pela data/ordem).
    """
    periods = None
    if isinstance(periods_payload, list):
        periods = periods_payload
    elif isinstance(periods_payload, dict):
        for k in ("periods", "results", "data"):
            if isinstance(periods_payload.get(k), list):
                periods = periods_payload.get(k)
                break
        if periods is None and all(isinstance(v, list) for v in periods_payload.values()):
            periods = next(iter(periods_payload.values()), [])
    else:
        periods = []

    if not periods:
        raise RuntimeError(f"periods_empty_or_unexpected_payload: {periods_payload}")

    def _period_sort_key(p: Any) -> Tuple[int, str]:
        if not isinstance(p, dict):
            return (0, "")
        for field in ("to", "end_date", "date_to", "until", "period_to"):
            v = p.get(field)
            if isinstance(v, str) and v:
                return (2, v)
        for field in ("from", "start_date", "date_from", "since", "period_from"):
            v = p.get(field)
            if isinstance(v, str) and v:
                return (1, v)
        key = p.get("key") or p.get("period_key") or p.get("id")
        if isinstance(key, str) and key:
            return (0, key)
        return (0, "")

    periods_sorted = sorted(periods, key=_period_sort_key)
    selected = periods_sorted[-1]

    if not isinstance(selected, dict):
        raise RuntimeError(f"period_item_unexpected: {selected}")

    key = selected.get("key") or selected.get("period_key") or selected.get("id")
    key = _as_str(key)
    if not key:
        raise RuntimeError(f"period_key_missing_in: {selected}")

    return key


def create_report(
    session: requests.Session,
    tp: TokenProvider,
    period_key: str,
    group: str,
    document_type: str,
    report_format: str,
) -> str:
    """
    POST /billing/integration/periods/key/{KEY}/reports
    Body: {"group":"ML","document_type":"BILL","report_format":"CSV"}
    Retorna fileId / file_id / id.
    """
    period_key = (period_key or "").strip()
    if not period_key:
        raise RuntimeError("period_key vazio (não foi possível selecionar período).")

    url = CREATE_REPORT_URL.format(key=period_key)
    payload = {
        "group": group,
        "document_type": document_type,
        "report_format": report_format,
    }

    r = _meli_post_json(session, url, tp, payload=payload, timeout=45)

    if r.status_code >= 400:
        raise RuntimeError(f"create_report_failed status={r.status_code} body={r.text[:1200]}")

    data = r.json() or {}
    file_id = data.get("fileId") or data.get("file_id") or data.get("id") or data.get("file")
    file_id = _as_str(file_id)
    if not file_id:
        raise RuntimeError(f"create_report_payload_unexpected: {data}")
    return file_id


def poll_report_ready(session: requests.Session, tp: TokenProvider, file_id: str, document_type: str) -> Dict[str, Any]:
    """
    GET /billing/integration/reports/{FILE_ID}/status?document_type=BILL
    Espera READY.
    """
    url = REPORT_STATUS_URL.format(file_id=file_id)
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_payload: Dict[str, Any] = {}

    while True:
        r = _meli_get(
            session,
            url,
            tp,
            params={"document_type": document_type},
            timeout=30,
            accept="application/json",
        )

        if r.status_code >= 400:
            raise RuntimeError(f"status_failed status={r.status_code} body={r.text[:1200]}")

        payload = r.json() or {}
        last_payload = payload

        status = _as_str(payload.get("status") or payload.get("state") or payload.get("report_status")).upper()

        if status == "READY":
            return payload

        if status in ("ERROR", "FAILED", "CANCELLED"):
            raise RuntimeError(f"report_failed status={status} payload={payload}")

        if time.time() >= deadline:
            raise RuntimeError(f"report_timeout last_payload={last_payload}")

        time.sleep(POLL_INTERVAL_SECONDS)


def download_report(session: requests.Session, tp: TokenProvider, file_id: str, document_type: str) -> Tuple[bytes, str]:
    """
    GET /billing/integration/reports/{FILE_ID}?document_type=BILL

    Pode retornar:
    - arquivo direto (CSV/XLSX)
    - ou JSON com url/download_url (link assinado).
    """
    url = REPORT_DOWNLOAD_URL.format(file_id=file_id)

    r = _meli_get(
        session,
        url,
        tp,
        params={"document_type": document_type},
        timeout=60,
        accept="*/*",
    )

    if r.status_code >= 400:
        raise RuntimeError(f"download_failed status={r.status_code} body={r.text[:1200]}")

    ct = (r.headers.get("Content-Type") or "").lower()

    # Caso devolva JSON com link de download
    if "application/json" in ct:
        try:
            data = r.json() or {}
        except Exception:
            data = {}

        dl_url = data.get("url") or data.get("download_url") or data.get("link")
        dl_url = _as_str(dl_url)
        if dl_url:
            # download externo (normalmente link assinado, sem precisar Authorization)
            r2 = session.get(dl_url, timeout=120, allow_redirects=True)
            if r2.status_code >= 400:
                raise RuntimeError(f"download_url_failed status={r2.status_code} body={r2.text[:800]}")
            ct2 = (r2.headers.get("Content-Type") or "").lower()
            return r2.content, ct2

        # se não tem link, retorna o próprio JSON como debug (mas isso é inesperado)
        raise RuntimeError(f"download_returned_json_without_url payload={data}")

    return r.content, ct


def guess_extension(report_format: str, content_type: str) -> str:
    rf = (report_format or "").upper()
    ct = (content_type or "").lower()

    if "spreadsheet" in ct or "xlsx" in ct or rf == "XLSX":
        return "xlsx"
    if "csv" in ct or "text/plain" in ct or "application/csv" in ct or rf == "CSV":
        return "csv"
    return "bin"


def save_file(content: bytes, out_dir: str, filename: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, filename)
    with open(path, "wb") as f:
        f.write(content)
    return path


def run() -> None:
    session = build_session()
    tp = TokenProvider(session)

    bundle = tp.get_access_token(force_refresh=True)
    token = bundle.access_token

    print("token_prefix=", _token_prefix(token))
    print("seller_id=", bundle.seller_id)

    group = (MELI_BILLING_GROUP or "ML").strip().upper()
    document_type = (MELI_BILLING_DOCUMENT_TYPE or "BILL").strip().upper()
    report_format = (MELI_BILLING_REPORT_FORMAT or "CSV").strip().upper()

    # 1) períodos (AGORA COM document_type)
    periods_payload = list_periods(session, tp, group=group, document_type=document_type)
    period_key = choose_period_key(periods_payload)
    print("period_key=", period_key)

    # 2) cria report
    file_id = create_report(
        session,
        tp,
        period_key=period_key,
        group=group,
        document_type=document_type,
        report_format=report_format,
    )
    print("file_id=", file_id)

    # 3) poll READY
    status_payload = poll_report_ready(session, tp, file_id=file_id, document_type=document_type)
    status_str = _as_str(status_payload.get("status") or status_payload.get("state") or "unknown")
    print("report_status=", status_str)

    # 4) download
    content, content_type = download_report(session, tp, file_id=file_id, document_type=document_type)

    # 5) salvar
    ext = guess_extension(report_format, content_type)
    ts = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    filename = _safe_filename(f"meli_sales_report_{group}_{document_type}_{period_key}_{file_id}_{ts}.{ext}")
    out_dir = (os.environ.get("MELI_REPORT_OUT_DIR", "/tmp") or "/tmp").strip()
    path = save_file(content, out_dir=out_dir, filename=filename)

    print("saved_path=", path)
    print("bytes=", len(content))
    print("content_type=", content_type or "unknown")


if __name__ == "__main__":
    run()
