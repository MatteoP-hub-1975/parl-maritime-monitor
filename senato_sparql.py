import json
import re
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from typing import Any, Dict, List, Tuple, Optional


SENATO_SPARQL_ENDPOINT = "https://dati.senato.it/sparql"

# User-Agent "browser-like" per ridurre probabilità di 403/WAF
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)


# ----------------------------
# HTTP helpers
# ----------------------------
def _read_http_error_body(e: urllib.error.HTTPError, max_chars: int = 800) -> str:
    try:
        body = e.read().decode("utf-8", errors="replace")
        body = re.sub(r"\s+", " ", body).strip()
        return body[:max_chars]
    except Exception:
        return "(impossibile leggere body)"


def _http_get(url: str, timeout_s: int = 25) -> str:
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.7,en;q=0.6",
        "Referer": "https://dati.senato.it/sparql",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _sparql_request_json(query: str, timeout_s: int = 25) -> Dict[str, Any]:
    """
    SPARQL via GET con format JSON.
    (Evitiamo varianti output=csv che possono cambiare il comportamento su Virtuoso.)
    """
    params = urllib.parse.urlencode(
        {
            "query": query,
            "format": "application/sparql-results+json",
        }
    )
    url = f"{SENATO_SPARQL_ENDPOINT}?{params}"

    headers = {
        "User-Agent": UA,
        "Accept": "application/sparql-results+json",
        "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.7,en;q=0.6",
        "Referer": "https://dati.senato.it/sparql",
    }

    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            payload = resp.read().decode("utf-8", errors="replace")
            return json.loads(payload)
    except urllib.error.HTTPError as e:
        body = _read_http_error_body(e)
        raise RuntimeError(f"HTTP {e.code} su SPARQL GET. Body: {body}")


def _request_with_retries(query: str, retries: int = 3, backoff_s: int = 4) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return _sparql_request_json(query=query, timeout_s=25)
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


# ----------------------------
# HTML -> text helper
# ----------------------------
class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._chunks: List[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self._chunks.append(data)

    def get_text(self) -> str:
        text = "\n".join(self._chunks)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{2,}", "\n", text)
        return text.strip()


def _html_to_text(html: str) -> str:
    p = _HTMLTextExtractor()
    p.feed(html)
    return p.get_text()


# ----------------------------
# Sindacato Ispettivo enrichment (show-doc)
# ----------------------------
_DATE_PUB_RE = re.compile(r"Pubblicat[oa]\s+il\s+(\d{1,2}/\d{1,2}/\d{4})", re.IGNORECASE)
_PRESENTATA_RE = re.compile(r"Presentata\s+da:\s*(.+)", re.IGNORECASE)
_GRUPPO_RE = re.compile(r"Gruppo:\s*(.+)", re.IGNORECASE)
_STATO_RE = re.compile(r"Stato:\s*(.+)", re.IGNORECASE)
_OGGETTO_RE = re.compile(r"(Oggetto|Titolo):\s*(.+)", re.IGNORECASE)


def _parse_ddmmyyyy(s: str) -> Optional[date]:
    try:
        d = datetime.strptime(s.strip(), "%d/%m/%Y").date()
        return d
    except Exception:
        return None


def _extract_first_line_after_label(text: str, regex: re.Pattern) -> str:
    m = regex.search(text)
    if not m:
        return ""
    # prendiamo solo la prima riga “sensata”
    line = m.group(1).strip()
    line = line.split("\n", 1)[0].strip()
    return line


def _extract_destinatario(text: str) -> str:
    """
    In molti Sindisp, subito sotto l'intestazione compare una riga tipo:
    "Al Ministro della salute ..."
    Oppure "Ai Ministri ..."
    Prendiamo la prima riga che inizia con Al/Ai/Alla/Alle.
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines[:80]:
        if re.match(r"^(Al|Ai|Alla|Alle|All')\s", ln):
            # evita falsi positivi banali
            if "Ministr" in ln or "Presidente" in ln or "Governo" in ln:
                return ln
    return ""


def _build_showdoc_url(urltesto: str, legislatura: str) -> str:
    """
    Trasforma ...loc/link.asp?...&id=1496xxx in show-doc?tipodoc=Sindisp&leg=19&id=...
    Se non riesce, restituisce urltesto.
    """
    try:
        parsed = urllib.parse.urlparse(urltesto)
        q = urllib.parse.parse_qs(parsed.query)
        act_id = (q.get("id") or [""])[0]
        leg = (q.get("leg") or [legislatura or "19"])[0]
        if act_id:
            return f"https://www.senato.it/show-doc?tipodoc=Sindisp&leg={leg}&id={act_id}"
    except Exception:
        pass
    return urltesto


def _enrich_sindisp_with_showdoc(item: Dict[str, str]) -> Dict[str, str]:
    """
    Scarica show-doc e aggiunge:
    - publication_date (dd/mm/yyyy)
    - destinatario
    - proponente
    - gruppo
    - stato
    - titolo (se presente)
    """
    urltesto = item.get("url", "").strip()
    leg = (item.get("leg", "") or "19").strip()

    show_url = _build_showdoc_url(urltesto, leg)
    item["url_direct"] = show_url

    try:
        html = _http_get(show_url, timeout_s=25)
        text = _html_to_text(html)

        # Data pubblicazione
        m = _DATE_PUB_RE.search(text)
        pub = m.group(1).strip() if m else ""
        item["publication_date"] = pub

        # Titolo / oggetto (se presente)
        m2 = _OGGETTO_RE.search(text)
        if m2:
            item["title"] = (m2.group(2) or "").strip()

        # Destinatario
        item["destinatario"] = _extract_destinatario(text)

        # Proponente / gruppo / stato
        item["proponente"] = _extract_first_line_after_label(text, _PRESENTATA_RE)
        item["gruppo"] = _extract_first_line_after_label(text, _GRUPPO_RE)
        item["stato"] = _extract_first_line_after_label(text, _STATO_RE)

    except Exception:
        # Se fallisce l'enrichment, lasciamo solo quanto già abbiamo
        pass

    return item


# ----------------------------
# Main fetch
# ----------------------------
def fetch_senato_last_48h(
    limit_each: int = 10,
    days: int = 2,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[str]]:
    """
    Returns (ddls, sindacato_ispettivo, warnings)

    Nota importante:
    - Per Sindacato Ispettivo filtriamo sulle 48h *in base a "Pubblicato il"*
      letto da show-doc (più coerente con quello che vedi nella ricerca manuale).
    - SPARQL serve solo per prendere una lista recente "candidata".
    """
    warnings: List[str] = []
    cutoff = date.today() - timedelta(days=days)

    # --------
    # DDL (lasciato “semplice” per ora)
    # --------
    q_ddl = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?s ?titolo ?numeroFase ?fase ?data
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/Ddl> .
  OPTIONAL {{ ?s <http://dati.senato.it/osr/titolo> ?titolo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numeroFase> ?numeroFase . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/fase> ?fase . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
}}
ORDER BY DESC(?data)
LIMIT {int(limit_each)}
""".strip()

    ddls: List[Dict[str, str]] = []
    try:
        res = _request_with_retries(q_ddl)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))
        for r in rows:
            act_uri = r.get("s", "")
            titolo = (r.get("titolo", "") or "").strip() or "(senza titolo)"
            numero = (r.get("numeroFase", "") or "").strip()
            fase = (r.get("fase", "") or "").strip()
            data_pres = (r.get("data", "") or "").strip()

            act_id = f"DDL {numero}" if numero else (fase if fase else "DDL")

            ddls.append(
                {
                    "branch": "Senato",
                    "act_id": act_id,
                    "title": titolo,
                    "url": act_uri,
                    "why": "SPARQL Senato (DDL)",
                    "date": data_pres,
                }
            )
    except Exception as e:
        warnings.append(f"Query DDL fallita su SPARQL Senato: {type(e).__name__}: {e}")

    # --------
    # Sindacato Ispettivo: SPARQL “leggero” + enrichment show-doc + filtro su pubblicazione
    # --------
    q_sind_light = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?s ?tipo ?numero ?data ?leg ?url
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/SindacatoIspettivo> .
  OPTIONAL {{ ?s <http://dati.senato.it/osr/tipo> ?tipo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numero> ?numero . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/legislatura> ?leg . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/URLTesto> ?url . }}
}}
ORDER BY DESC(?data)
LIMIT 200
""".strip()

    sind: List[Dict[str, str]] = []
    try:
        res = _request_with_retries(q_sind_light)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))

        # 1) normalizza base
        candidates: List[Dict[str, str]] = []
        for r in rows:
            tipo = (r.get("tipo", "") or "").strip()
            numero = (r.get("numero", "") or "").strip()
            data_pres = (r.get("data", "") or "").strip()
            leg = (r.get("leg", "") or "19").strip()
            urltesto = (r.get("url", "") or "").strip()

            # filtra per i tipi che ti interessano (ma usando STR, non literal typed)
            if tipo not in {
                "Interrogazione con richiesta di risposta scritta",
                "Interrogazione",
                "Mozione",
                "Interpellanza",
                "Risoluzione in Assemblea",
                "Risoluzione autonoma in commissione",
            }:
                continue

            candidates.append(
                {
                    "branch": "Senato",
                    "tipo": tipo,
                    "numero": numero,
                    "date_presentazione": data_pres,
                    "leg": leg,
                    "url": urltesto,
                    "why": "SPARQL Senato (Sindacato ispettivo, light) + show-doc",
                }
            )

        # 2) enrichment show-doc + filtro su "Pubblicato il" nelle ultime 48h
        for item in candidates:
            enriched = _enrich_sindisp_with_showdoc(item)

            pub_s = (enriched.get("publication_date") or "").strip()
            pub_d = _parse_ddmmyyyy(pub_s) if pub_s else None

            # Se non riesco a leggere la data pubblicazione, lo tengo “borderline”
            if pub_d is None:
                continue

            if pub_d >= cutoff:
                # act_id per output: "Interrogazione ... 3-xxxxx"
                act_id = f"{enriched.get('tipo','Sindisp')} {enriched.get('numero','')}".strip()

                sind.append(
                    {
                        "branch": "Senato",
                        "act_id": act_id,
                        "title": (enriched.get("title") or "").strip(),
                        "destinatario": (enriched.get("destinatario") or "").strip(),
                        "proponente": (enriched.get("proponente") or "").strip(),
                        "gruppo": (enriched.get("gruppo") or "").strip(),
                        "stato": (enriched.get("stato") or "").strip(),
                        "date_pubblicazione": pub_s,
                        "url": enriched.get("url_direct") or enriched.get("url") or "",
                        "why": enriched.get("why") or "",
                    }
                )

        # limita output finale
        sind = sind[: int(limit_each)]

    except Exception as e:
        warnings.append(f"Query Sindacato Ispettivo fallita su SPARQL Senato: {type(e).__name__}: {e}")

    return ddls, sind, warnings
