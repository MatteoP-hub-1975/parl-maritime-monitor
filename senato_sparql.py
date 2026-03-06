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
    # remove script/style
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    # remove tags
    text = re.sub(r"(?s)<[^>]+>", " ", html)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    # normalize spaces
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


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

    # 1) GET (molto “browser-like”)
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
        # se non è 403/400, rilancio; altrimenti provo POST
        if he.code not in (400, 403):
            raise

    # 2) POST (fallback)
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
# Enrichment parsers
# -------------------------

_IT_MONTHS = {
    "gennaio": "01",
    "febbraio": "02",
    "marzo": "03",
    "aprile": "04",
    "maggio": "05",
    "giugno": "06",
    "luglio": "07",
    "agosto": "08",
    "settembre": "09",
    "ottobre": "10",
    "novembre": "11",
    "dicembre": "12",
}

def _it_date_to_iso(s: str) -> str:
    # "3 marzo 2026" -> "2026-03-03" (best effort)
    s = s.strip().lower()
    m = re.search(r"\b(\d{1,2})\s+([a-zà]+)\s+(\d{4})\b", s, re.I)
    if not m:
        return ""
    dd = m.group(1).zfill(2)
    mm = _IT_MONTHS.get(m.group(2), "")
    yyyy = m.group(3)
    if not mm:
        return ""
    return f"{yyyy}-{mm}-{dd}"


def _enrich_ddl_commissione(url: str) -> str:
    try:
        html = _http_get(url, timeout_s=25)
        text = _strip_html_to_text(html)

        # prova 1: frase “Assegnato alla …”
        m = re.search(r"Assegnat[oa]\s+alla\s+([^.\n]+)", text, re.I)
        if m:
            return m.group(1).strip()

        # prova 2: prima occorrenza “Xª Commissione …”
        m2 = re.search(r"\b(\d{1,2}[ªa]\s+Commissione[^.\n]+)", text)
        if m2:
            return m2.group(1).strip()

        return ""
    except Exception:
        return ""


def _normalize_sindisp_url(url: str) -> str:
    """
    Converte link tipo:
    http://www.senato.it/loc/link.asp?tipodoc=sindisp&leg=19&id=1496581
    -> https://www.senato.it/show-doc?id=1496581&leg=19&tipodoc=Sindisp
    """
    if not url:
        return ""
    if "show-doc" in url:
        # normalizza parametri ordine
        parsed = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qs(parsed.query)
        _id = (q.get("id") or [""])[0]
        leg = (q.get("leg") or [""])[0]
        tip = (q.get("tipodoc") or ["Sindisp"])[0]
        if _id and leg:
            return f"{SENATO_BASE}/show-doc?id={_id}&leg={leg}&tipodoc={tip}"
        return url

    if "loc/link.asp" in url and "tipodoc=sindisp" in url.lower():
        parsed = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qs(parsed.query)
        _id = (q.get("id") or [""])[0]
        leg = (q.get("leg") or [""])[0]
        if _id and leg:
            return f"{SENATO_BASE}/show-doc?id={_id}&leg={leg}&tipodoc=Sindisp"

    return url


def _enrich_sindisp_showdoc(url: str) -> Dict[str, str]:
    """
    Estrae da show-doc:
    - data_pubblicazione (ISO)
    - proponenti (testo)
    - destinatario (testo)
    """
    out: Dict[str, str] = {"data_pubblicazione": "", "proponenti": "", "destinatario": ""}

    try:
        html = _http_get(url, timeout_s=25)
        text = _strip_html_to_text(html)

        # Pubblicato il 3 marzo 2026, ...
        m = re.search(r"Pubblicat[oa]\s+il\s+([^,]+)", text, re.I)
        if m:
            iso = _it_date_to_iso(m.group(1))
            out["data_pubblicazione"] = iso or m.group(1).strip()

        # Riga tipo: "MURELLI - Al Ministro della salute. -"
        # Proponente: prima del primo " - "
        # Destinatario: tra " - " e ". -"
        m2 = re.search(r"\n([A-ZÀ-Ü' ]{3,})\s+-\s+(A(?:l|lla|ll')\s+[^.]+)\.\s+-", text)
        if m2:
            out["proponenti"] = m2.group(1).strip()
            out["destinatario"] = m2.group(2).strip()
        else:
            # fallback: cerca solo destinatario
            m3 = re.search(r"-\s+(A(?:l|lla|ll')\s+[^.]+)\.\s+-", text)
            if m3:
                out["destinatario"] = m3.group(1).strip()

        return out
    except Exception:
        return out


# -------------------------
# Public API
# -------------------------

def fetch_senato_last_48h(
    limit_each: int = 10,
    days: int = 2,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[str]]:
    """
    Returns (ddls, sindacato_ispettivo, warnings)

    DDL: include numero ddl, titolo, data presentazione, iniziativa, stato, commissione, link scheda-ddl
    Sindisp: include tipo, numero, data presentazione, data pubblicazione (se trovata), proponente, destinatario, stato/esito, link show-doc
    """
    warnings: List[str] = []

    now = datetime.now(TZ_ROME) if TZ_ROME else datetime.now()
    start_date = (now - timedelta(days=days)).date().isoformat()  # YYYY-MM-DD

    # --- DDL (minimal SPARQL, fields certi) ---
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

    # --- Sindacato ispettivo (SPARQL “leggero” + enrichment show-doc) ---
    wanted_tipi = [
        "Interrogazione con richiesta di risposta scritta",
        "Interrogazione",
        "Mozione",
        "Interpellanza",
        "Risoluzione in Assemblea",
        "Risoluzione autonoma in commissione",
    ]
    tipi_list = ", ".join([f'"{t}"' for t in wanted_tipi])

    q_sind = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?s ?tipo ?numero ?data ?url ?esito
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/SindacatoIspettivo> .
  OPTIONAL {{ ?s <http://dati.senato.it/osr/tipo> ?tipo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numero> ?numero . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/URLTesto> ?url . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/esito> ?esito . }}

  FILTER(BOUND(?data))
  FILTER(STR(?data) >= "{start_date}")

  FILTER( STR(?tipo) IN ({tipi_list}) )
}}
ORDER BY DESC(?data)
LIMIT {int(limit_each)}
""".strip()

    ddls: List[Dict[str, str]] = []
    sind: List[Dict[str, str]] = []

    # DDL
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

            url = f"{SENATO_BASE}/leggi-e-documenti/disegni-di-legge/scheda-ddl?did={id_fase}" if id_fase else (r.get("ddl", "") or "").strip()

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

    # Sindacato ispettivo
    try:
        res = _request_with_retries(q_sind)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))

        for r in rows:
            tipo = (r.get("tipo", "") or "").strip() or "Sindacato ispettivo"
            numero = (r.get("numero", "") or "").strip()
            data_pres = (r.get("data", "") or "").strip()
            esito = (r.get("esito", "") or "").strip()

            url_raw = (r.get("url", "") or "").strip()
            url = _normalize_sindisp_url(url_raw) or (r.get("s", "") or "").strip()

            enrich = _enrich_sindisp_showdoc(url) if url else {"data_pubblicazione": "", "proponenti": "", "destinatario": ""}

            sind.append(
                {
                    "branch": "Senato",
                    "tipo": tipo,
                    "numero": numero,
                    "title": "",  # spesso non esiste un “titolo” separato nelle show-doc
                    "url": url,
                    "date_presentazione": data_pres,
                    "data_pubblicazione": enrich.get("data_pubblicazione", ""),
                    "destinatario": enrich.get("destinatario", ""),
                    "proponenti": enrich.get("proponenti", ""),
                    "gruppo": "",  # best effort: lo aggiungiamo dopo (serve fetch pagina senatore)
                    "stato": esito,
                }
            )
    except Exception as e:
        warnings.append(f"Query Sindacato Ispettivo fallita su SPARQL Senato: {type(e).__name__}: {e}")

    return ddls, sind, warnings
