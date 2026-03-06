import json
import re
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from html import unescape
from typing import Any, Dict, List, Tuple, Optional

try:
    from zoneinfo import ZoneInfo
    TZ_ROME = ZoneInfo("Europe/Rome")
except Exception:
    TZ_ROME = None

SENATO_SPARQL_ENDPOINT = "https://dati.senato.it/sparql"
SENATO_BASE = "https://www.senato.it"


# -------------------------
# HTTP helpers
# -------------------------

def _http_get(url: str, timeout_s: int = 25) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        "Connection": "close",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _strip_html_to_text(html: str) -> str:
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", " ", html)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def _url_looks_ok(url: str, timeout_s: int = 12) -> bool:
    """
    True se la pagina sembra valida (no 403/404 e no pagina errore).
    Legge solo i primi byte per essere leggero.
    """
    if not url:
        return False

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        "Connection": "close",
        "Range": "bytes=0-2048",
    }

    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            chunk = resp.read(2048).decode("utf-8", errors="replace").lower()
    except urllib.error.HTTPError:
        return False
    except Exception:
        return False

    bad_markers = [
        "errore_403",
        "accesso negato",
        "pagina non trovata",
        "http 403",
        "404",
    ]
    return not any(b in chunk for b in bad_markers)


# -------------------------
# SPARQL helpers
# -------------------------

def _sparql_request_json(query: str, timeout_s: int = 25) -> Dict[str, Any]:
    query = query.replace("\xa0", " ")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/sparql-results+json",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        "Referer": SENATO_SPARQL_ENDPOINT,
        "Origin": "https://dati.senato.it",
        "Connection": "close",
    }

    # 1) GET (preferito)
    try:
        params = urllib.parse.urlencode(
            {"query": query, "format": "application/sparql-results+json"}
        )
        url = f"{SENATO_SPARQL_ENDPOINT}?{params}"
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            payload = resp.read().decode("utf-8", errors="replace")
            return json.loads(payload)
    except urllib.error.HTTPError as he:
        # fallback a POST solo su errori comuni
        if he.code not in (400, 403):
            raise

    # 2) POST fallback
    data = urllib.parse.urlencode({"query": query}).encode("utf-8")
    headers_post = dict(headers)
    headers_post["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"

    try:
        req = urllib.request.Request(
            SENATO_SPARQL_ENDPOINT, data=data, headers=headers_post, method="POST"
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            payload = resp.read().decode("utf-8", errors="replace")
            return json.loads(payload)
    except urllib.error.HTTPError as he:
        try:
            body = he.read().decode("utf-8", errors="replace")
            body = body.strip().replace("\n", " ")[:500]
        except Exception:
            body = "(impossibile leggere body)"
        raise RuntimeError(f"HTTP {he.code} su SPARQL. Body: {body}") from he


def _request_with_retries(
    query: str, timeout_s: int = 25, retries: int = 3, backoff_s: int = 4
) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return _sparql_request_json(query=query, timeout_s=timeout_s)
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_s * attempt)
            else:
                raise
    raise last_err if last_err else RuntimeError("SPARQL failed without exception")


def _bindings_to_rows(bindings: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for b in bindings:
        row: Dict[str, str] = {}
        for k, v in b.items():
            row[k] = v.get("value", "")
        rows.append(row)
    return rows


# -------------------------
# Utility: link Sindisp robusto
# -------------------------

def _extract_sindisp_doc_id(url: str) -> str:
    """
    Estrae l'id SOLO se presente come parametro id=... nella querystring.
    (Evitiamo i falsi positivi e gli id "fantasma".)
    """
    if not url:
        return ""
    try:
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        if "id" in qs and qs["id"]:
            return (qs["id"][0] or "").strip()
    except Exception:
        pass

    m = re.search(r"[?&]id=(\d+)", url)
    return m.group(1) if m else ""


def _build_sindisp_showdoc_url(doc_id: str, leg: str) -> str:
    leg = (leg or "19").strip()
    doc_id = (doc_id or "").strip()
    if not doc_id:
        return ""
    return f"{SENATO_BASE}/show-doc?tipodoc=Sindisp&leg={leg}&id={doc_id}"


# -------------------------
# Enrichment DDL: commissione
# -------------------------

def _enrich_ddl_commissione(url: str) -> str:
    try:
        html = _http_get(url, timeout_s=25)
        text = _strip_html_to_text(html)

        m = re.search(r"Assegnat[oa]\s+alla\s+([^.\n]+)", text, re.I)
        if m:
            return m.group(1).strip()

        m2 = re.search(r"\b(\d{1,2}[ªa]\s+Commissione[^.\n]+)", text)
        if m2:
            return m2.group(1).strip()

        return ""
    except Exception:
        return ""


# -------------------------
# Public API
# -------------------------

def fetch_senato_last_48h(
    limit_each: int = 200,
    days: int = 2,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[str]]:
    warnings: List[str] = []

    now = datetime.now(TZ_ROME) if TZ_ROME else datetime.now()
    start_date = (now - timedelta(days=days)).date().isoformat()  # YYYY-MM-DD

    # ---- DDL ----
    q_ddl = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?ddl ?idFase ?numeroFase ?data ?titolo ?iniziativa ?stato
WHERE {{
  ?ddl rdf:type <http://dati.senato.it/osr/Ddl> .
  OPTIONAL {{ ?ddl <http://dati.senato.it/osr/idFase> ?idFase . }}
  OPTIONAL {{ ?ddl <http://dati.senato.it/osr/numeroFase> ?numeroFase . }}
  OPTIONAL {{ ?ddl <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?ddl <http://dati.senato.it/osr/titolo> ?titolo . }}
  OPTIONAL {{ ?ddl <http://dati.senato.it/osr/descrIniziativa> ?iniziativa . }}
  OPTIONAL {{ ?ddl <http://dati.senato.it/osr/statoDdl> ?stato . }}

  FILTER(BOUND(?data))
  FILTER(STR(?data) >= "{start_date}")
}}
ORDER BY DESC(?data)
LIMIT {int(limit_each)}
""".strip()

    # ---- Sindacato Ispettivo ----
    q_sind = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?s ?tipo ?numero ?data ?url ?leg ?esito
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/SindacatoIspettivo> .
  OPTIONAL {{ ?s <http://dati.senato.it/osr/tipo> ?tipo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numero> ?numero . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/URLTesto> ?url . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/legislatura> ?leg . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/esito> ?esito . }}

  FILTER(BOUND(?data))
  FILTER(STR(?data) >= "{start_date}")

  FILTER(BOUND(?tipo))
  FILTER(
    CONTAINS(LCASE(STR(?tipo)), "interrogazione") ||
    CONTAINS(LCASE(STR(?tipo)), "interpellanza") ||
    CONTAINS(LCASE(STR(?tipo)), "mozione") ||
    CONTAINS(LCASE(STR(?tipo)), "risoluzione")
  )
}}
ORDER BY DESC(?data)
LIMIT 500
""".strip()

    ddls: List[Dict[str, str]] = []
    sind: List[Dict[str, str]] = []

    # ---- DDL fetch ----
    try:
        res = _request_with_retries(q_ddl)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))

        for r in rows:
            id_fase = (r.get("idFase", "") or "").strip()
            numero_fase = (r.get("numeroFase", "") or "").strip()
            titolo = (r.get("titolo", "") or "").strip() or "(senza titolo)"
            data_pres = (r.get("data", "") or "").strip()
            iniziativa = (r.get("iniziativa", "") or "").strip()
            stato = (r.get("stato", "") or "").strip()

            url = (
                f"{SENATO_BASE}/leggi-e-documenti/disegni-di-legge/scheda-ddl?did={id_fase}"
                if id_fase
                else (r.get("ddl", "") or "").strip()
            )

            commissione = _enrich_ddl_commissione(url) if url else ""

            ddls.append(
                {
                    "branch": "Senato",
                    "act_id": f"DDL {numero_fase}".strip() if numero_fase else "DDL",
                    "title": titolo,
                    "url": url,
                    "date_presentazione": data_pres,
                    "iniziativa": iniziativa,
                    "stato": stato,
                    "commissione": commissione,
                }
            )

    except Exception as e:
        warnings.append(f"Query DDL fallita su SPARQL Senato: {type(e).__name__}: {e}")

    # ---- Sindacato fetch (dedup + show-doc solo se valido) ----
    try:
        res = _request_with_retries(q_sind)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))

        # dedup per (leg, numero) -> così eliminiamo duplicati anche se cambia "tipo"
        sind_map: Dict[Tuple[str, str], Dict[str, str]] = {}
        showdoc_ok_cache: Dict[str, bool] = {}

        def _score_url(u: str) -> int:
            if not u:
                return 0
            if "show-doc" in u:
                return 3
            if "loc/link.asp" in u:
                return 2
            return 1

        for r in rows:
            act_uri = (r.get("s", "") or "").strip()
            tipo = (r.get("tipo", "") or "").strip() or "Sindacato ispettivo"
            numero = (r.get("numero", "") or "").strip()
            data_pres = (r.get("data", "") or "").strip()
            esito = (r.get("esito", "") or "").strip()
            leg = (r.get("leg", "") or "19").strip()
            urltesto = (r.get("url", "") or "").strip()

            # doc_id SOLO da URLTesto (non da act_uri)
            doc_id = _extract_sindisp_doc_id(urltesto)
            url_showdoc = _build_sindisp_showdoc_url(doc_id, leg) if doc_id else ""

            url_direct = ""
            if url_showdoc:
                ok = showdoc_ok_cache.get(url_showdoc)
                if ok is None:
                    ok = _url_looks_ok(url_showdoc)
                    showdoc_ok_cache[url_showdoc] = ok
                if ok:
                    url_direct = url_showdoc

            # fallback: se show-doc non ok -> URLTesto -> act_uri
            if not url_direct:
                url_direct = urltesto or act_uri

            item = {
                "branch": "Senato",
                "tipo": tipo,
                "numero": numero,
                "title": "",
                "url": url_direct,
                "date_presentazione": data_pres,
                "destinatario": "",
                "proponenti": "",
                "gruppo": "",
                "stato": esito,
            }

            key = (leg, numero)
            prev = sind_map.get(key)

            if prev is None:
                sind_map[key] = item
            else:
                # preferisci link migliore
                if _score_url(item["url"]) > _score_url(prev["url"]):
                    sind_map[key] = item
                elif _score_url(item["url"]) == _score_url(prev["url"]):
                    # a parità di link, preferisci tipo più specifico (stringa più lunga)
                    if len(item.get("tipo", "")) > len(prev.get("tipo", "")):
                        sind_map[key] = item

        sind = list(sind_map.values())
        sind.sort(key=lambda x: (x.get("date_presentazione") or ""), reverse=True)

    except Exception as e:
        warnings.append(f"Query Sindacato Ispettivo fallita su SPARQL Senato: {type(e).__name__}: {e}")
        sind = []

    return ddls, sind, warnings
