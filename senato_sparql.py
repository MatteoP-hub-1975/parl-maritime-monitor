from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple

SENATO_SPARQL_ENDPOINT = "https://dati.senato.it/sparql"


# ---------- helpers HTTP / SPARQL ----------

def _read_http_error_body(e: urllib.error.HTTPError, max_chars: int = 500) -> str:
    try:
        b = e.read().decode("utf-8", errors="replace")
        b = re.sub(r"\s+", " ", b).strip()
        return b[:max_chars]
    except Exception:
        return "(impossibile leggere body)"


def _sparql_get_json(query: str, timeout_s: int = 25) -> Dict[str, Any]:
    headers = {
        "Accept": "application/sparql-results+json",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) parl-maritime-monitor/0.2",
    }
    params = urllib.parse.urlencode(
        {"query": query, "format": "application/sparql-results+json"},
        quote_via=urllib.parse.quote,
    )
    url = f"{SENATO_SPARQL_ENDPOINT}?{params}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _sparql_post_json(query: str, timeout_s: int = 25) -> Dict[str, Any]:
    # Solo come fallback: su GH Actions può essere bloccato
    headers = {
        "Accept": "application/sparql-results+json",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) parl-maritime-monitor/0.2",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    data = urllib.parse.urlencode({"query": query}).encode("utf-8")
    req = urllib.request.Request(SENATO_SPARQL_ENDPOINT, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _sparql_json(query: str, timeout_s: int = 25) -> Dict[str, Any]:
    # GET-first (perché POST viene spesso 403 da GitHub Actions)
    try:
        return _sparql_get_json(query, timeout_s=timeout_s)
    except urllib.error.HTTPError as e_get:
        body_get = _read_http_error_body(e_get)
        # prova POST una sola volta
        try:
            return _sparql_post_json(query, timeout_s=timeout_s)
        except urllib.error.HTTPError as e_post:
            body_post = _read_http_error_body(e_post)
            raise RuntimeError(
                f"SPARQL GET HTTP {e_get.code}: {body_get} | POST HTTP {e_post.code}: {body_post}"
            ) from e_post


def _request_with_retries(query: str, timeout_s: int = 25, retries: int = 3, backoff_s: int = 4) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return _sparql_json(query=query, timeout_s=timeout_s)
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


# ---------- link builders ----------

def _ddl_did_from_uri(uri: str) -> str:
    m = re.search(r"/ddl/(\d+)", uri)
    return m.group(1) if m else uri.rsplit("/", 1)[-1]


def _senato_ddl_page(did: str) -> str:
    return f"https://www.senato.it/leggi-e-documenti/disegni-di-legge/scheda-ddl?did={did}"


def _sindisp_showdoc_from_url(url: str, fallback_leg: str = "19") -> str:
    # es: http://www.senato.it/loc/link.asp?tipodoc=sindisp&leg=19&id=1496581
    try:
        p = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(p.query)
        leg = (qs.get("leg", [fallback_leg])[0] or fallback_leg).strip()
        doc_id = (qs.get("id", [""])[0] or "").strip()
        tipodoc = (qs.get("tipodoc", ["Sindisp"])[0] or "Sindisp").strip()
        tipodoc = "Sindisp" if tipodoc.lower().startswith("sind") else tipodoc
        if doc_id:
            return f"https://www.senato.it/show-doc?tipodoc={tipodoc}&leg={leg}&id={doc_id}"
    except Exception:
        pass

    # fallback: prova a estrarre un id numerico
    m = re.search(r"(\d{6,})", url)
    doc_id = m.group(1) if m else ""
    return f"https://www.senato.it/show-doc?tipodoc=Sindisp&leg={fallback_leg}&id={doc_id}" if doc_id else url


# ---------- fallback parsing show-doc (se SPARQL dettagli fallisce) ----------

class _HTMLText(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: List[str] = []
        self._newline_tags = {"br", "p", "div", "li", "tr", "h1", "h2", "h3"}

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in self._newline_tags:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def get_text(self) -> str:
        txt = "".join(self.parts)
        txt = re.sub(r"\n{3,}", "\n\n", txt)
        return txt


def _fetch_showdoc_text(url: str, timeout_s: int = 25) -> str:
    headers = {
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) parl-maritime-monitor/0.2",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        html_bytes = resp.read()
    parser = _HTMLText()
    parser.feed(html_bytes.decode("utf-8", errors="replace"))
    return parser.get_text()


def _parse_sindisp_from_showdoc(text: str) -> Dict[str, str]:
    # Cerca riga tipo: "COGNOME (GRUPPO) - Al Ministro ..."
    proponente = ""
    gruppo = ""
    destinatario = ""
    titolo = ""

    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]

    dash_line = next((ln for ln in lines[:50] if " - Al " in ln or " - Alla " in ln), "")
    if dash_line:
        m = re.match(r"^([A-ZÀ-Ü'\s]+?)(?:\s*\(([^)]+)\))?\s*-\s*(Al(?:la)?\s+.+)$", dash_line)
        if m:
            proponente = m.group(1).strip()
            gruppo = (m.group(2) or "").strip()
            destinatario = m.group(3).strip().rstrip(".")
        else:
            parts = dash_line.split(" - ", 1)
            proponente = parts[0].strip()
            destinatario = parts[1].strip().rstrip(".") if len(parts) > 1 else ""

    # Stato “minimo”: se c'è una sezione RISPOSTA nella pagina
    stato = "Risposta presente" if re.search(r"\bRISPOSTA\b", text, flags=re.IGNORECASE) else ""

    # Titolo/oggetto se esiste una riga "Oggetto: ..."
    obj_line = next((ln for ln in lines[:160] if ln.lower().startswith("oggetto")), "")
    if obj_line and ":" in obj_line:
        titolo = obj_line.split(":", 1)[-1].strip()

    return {"proponente": proponente, "gruppo": gruppo, "destinatario": destinatario, "stato": stato, "titolo": titolo}


def _fetch_sindisp_details_via_sparql(act_uri: str) -> Dict[str, str]:
    # Query molto corta “per singolo atto” -> evita URL lunghi
    q = f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?titolo ?dest ?prop ?grLabel ?stato
WHERE {{
  VALUES ?s {{ <{act_uri}> }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/titolo> ?titolo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/destinatario> ?dest . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/indirizzataA> ?dest . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/descrIniziativa> ?prop . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/gruppo> ?gr . OPTIONAL {{ ?gr rdfs:label ?grLabel }} }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/gruppoParlamentare> ?gr2 . OPTIONAL {{ ?gr2 rdfs:label ?grLabel }} }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/esito> ?stato . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/stato> ?stato . }}
}}
LIMIT 1
""".strip()

    res = _request_with_retries(q, retries=2, backoff_s=2)
    rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))
    if not rows:
        return {}
    r = rows[0]
    return {
        "titolo": (r.get("titolo", "") or "").strip(),
        "destinatario": (r.get("dest", "") or "").strip(),
        "proponente": (r.get("prop", "") or "").strip(),
        "gruppo": (r.get("grLabel", "") or "").strip(),
        "stato": (r.get("stato", "") or "").strip(),
    }


# ---------- main entrypoint ----------

def fetch_senato_last_48h(
    limit_each: int = 10,
    days: int = 2,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[str]]:
    """
    Returns (ddls, sindacato_ispettivo, warnings)

    NOTE: la parte “DDL” qui continua a funzionare come prima, ma ora:
      - link DDL = scheda-ddl?did=...
      - sindacato: niente query lunghissima -> niente 403 “da URL lungo”
    """
    warnings: List[str] = []
    start_date = (date.today() - timedelta(days=days)).isoformat()  # YYYY-MM-DD

    # --- DDL (quella che ti funzionava) ---
    q_ddl = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?s ?titolo ?numeroFase ?fase ?data ?stato ?iniziativa
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/Ddl> .
  OPTIONAL {{ ?s <http://dati.senato.it/osr/titolo> ?titolo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numeroFase> ?numeroFase . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/fase> ?fase . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/statoDdl> ?stato . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/descrIniziativa> ?iniziativa . }}

  FILTER(BOUND(?data))
  FILTER(STR(?data) >= "{start_date}")
}}
ORDER BY DESC(?data)
LIMIT {int(limit_each)}
""".strip()

    # --- Sindacato (LISTA CORTA) ---
    q_sind_list = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>

SELECT ?s ?tipo ?numero ?data ?url ?leg
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/SindacatoIspettivo> .

  OPTIONAL {{ ?s <http://dati.senato.it/osr/tipo> ?tipo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numero> ?numero . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/URLTesto> ?url . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/legislatura> ?leg . }}

  FILTER(BOUND(?data))
  FILTER(STR(?data) >= "{start_date}")

  FILTER(
    ?tipo = "Interrogazione con richiesta di risposta scritta"^^xsd:string ||
    ?tipo = "Interrogazione"^^xsd:string ||
    ?tipo = "Mozione"^^xsd:string ||
    ?tipo = "Interpellanza"^^xsd:string ||
    ?tipo = "Risoluzione in Assemblea"^^xsd:string ||
    ?tipo = "Risoluzione autonoma in commissione"^^xsd:string
  )
}}
ORDER BY DESC(?data)
LIMIT {int(limit_each)}
""".strip()

    ddls: List[Dict[str, str]] = []
    sind: List[Dict[str, str]] = []

    # ---- DDL ----
    try:
        res = _request_with_retries(q_ddl)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))
        for r in rows:
            act_uri = (r.get("s", "") or "").strip()
            titolo = (r.get("titolo", "") or "").strip() or "(senza titolo)"
            numero = (r.get("numeroFase", "") or "").strip()
            fase = (r.get("fase", "") or "").strip()
            data_pres = (r.get("data", "") or "").strip()
            stato = (r.get("stato", "") or "").strip()
            iniziativa = (r.get("iniziativa", "") or "").strip()

            act_id = f"DDL {numero}".strip() if numero else (fase if fase else "DDL")
            did = _ddl_did_from_uri(act_uri) if act_uri else ""
            page = _senato_ddl_page(did) if did else act_uri

            ddls.append(
                {
                    "branch": "Senato",
                    "act_id": act_id,
                    "title": titolo,
                    "url": page,            # link “scheda-ddl?did=…”
                    "date": data_pres,      # data presentazione
                    "initiative": iniziativa,
                    "state": stato,
                }
            )
    except Exception as e:
        warnings.append(f"Query DDL fallita su SPARQL Senato: {type(e).__name__}: {e}")

    # ---- Sindacato Ispettivo ----
    try:
        res = _request_with_retries(q_sind_list)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))

        for r in rows:
            act_uri = (r.get("s", "") or "").strip()
            tipo = (r.get("tipo", "") or "").strip() or "Sindacato ispettivo"
            numero = (r.get("numero", "") or "").strip()
            data_pres = (r.get("data", "") or "").strip()
            url = (r.get("url", "") or "").strip()
            leg = (r.get("leg", "") or "").strip() or "19"

            showdoc = _sindisp_showdoc_from_url(url or act_uri, fallback_leg=leg)

            details: Dict[str, str] = {}
            # 1) prova dettagli via SPARQL per singolo atto (query corta)
            try:
                if act_uri:
                    details = _fetch_sindisp_details_via_sparql(act_uri)
            except Exception:
                details = {}

            # 2) fallback: parse show-doc
            if not details:
                try:
                    txt = _fetch_showdoc_text(showdoc)
                    details = _parse_sindisp_from_showdoc(txt)
                except Exception:
                    details = {}

            sind.append(
                {
                    "branch": "Senato",
                    "act_id": f"{tipo} {numero}".strip(),
                    "title": (details.get("titolo") or "").strip(),   # se presente
                    "url": showdoc,                                   # link diretto show-doc
                    "date": data_pres,                                # data presentazione
                    "type": tipo,
                    "number": numero,
                    "to": (details.get("destinatario") or "").strip(),
                    "proposer": (details.get("proponente") or "").strip(),
                    "group": (details.get("gruppo") or "").strip(),
                    "state": (details.get("stato") or "").strip(),
                }
            )

            # micro rate-limit: evitiamo di martellare il sito
            time.sleep(0.6)

    except Exception as e:
        warnings.append(f"Query Sindacato Ispettivo fallita su SPARQL Senato: {type(e).__name__}: {e}")

    return ddls, sind, warnings
