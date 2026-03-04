import json
import re
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

SENATO_SPARQL_ENDPOINT = "https://dati.senato.it/sparql"

RE_NUM_TAIL = re.compile(r"(\d+)$")


def _sparql_request_json(query: str, timeout_s: int = 25) -> Dict[str, Any]:
    """
    Try SPARQL POST; if 403 then try GET with multiple Virtuoso-style variants.
    Return parsed JSON.
    """
    headers_common = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "parl-maritime-monitor/0.1 (GitHub Actions)",
    }

    # 1) Try POST
    try:
        data = urllib.parse.urlencode({"query": query}).encode("utf-8")
        headers_post = {
            **headers_common,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
        req = urllib.request.Request(
            SENATO_SPARQL_ENDPOINT,
            data=data,
            headers=headers_post,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload)

    except urllib.error.HTTPError as e:
        # Only fallback on forbidden
        if e.code != 403:
            raise

    # 2) Fallback GET (Virtuoso-style variants)
    get_variants = [
        {"query": query},
        {"query": query, "output": "json"},
        {"query": query, "output": "application/sparql-results+json"},
        {"query": query, "output": "application/json"},
        {"query": query, "output": "text/csv"},
    ]

    last_err: Exception | None = None
    for params_dict in get_variants:
        try:
            params = urllib.parse.urlencode(params_dict)
            url = f"{SENATO_SPARQL_ENDPOINT}?{params}"
            req = urllib.request.Request(url, headers=headers_common, method="GET")
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                payload = resp.read().decode("utf-8")
                return json.loads(payload)

        except urllib.error.HTTPError as he:
            # prova a leggere il body dell’errore (spesso contiene il motivo del 400)
            try:
                err_body = he.read().decode("utf-8", errors="replace")
                err_body = err_body.strip().replace("\n", " ")[:600]
            except Exception:
                err_body = "(impossibile leggere body)"
            last_err = RuntimeError(
                f"HTTP {he.code} su GET variant {params_dict}. Body: {err_body}"
            )
            continue

        except Exception as e2:
            last_err = e2
            continue

    raise last_err if last_err else RuntimeError("SPARQL GET fallback failed without exception")


def _request_with_retries(
    query: str,
    timeout_s: int = 25,
    retries: int = 3,
    backoff_s: int = 5,
) -> Dict[str, Any]:
    last_err: Exception | None = None
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


def _extract_numeric_tail(uri: str) -> str:
    m = RE_NUM_TAIL.search(uri or "")
    return m.group(1) if m else ""


def _senato_ddl_link(id_fase: str) -> str:
    # link richiesto dall’utente
    return f"https://www.senato.it/leggi-e-documenti/disegni-di-legge/scheda-ddl?did={id_fase}" if id_fase else ""


def _senato_sindisp_link(leg: str, doc_id: str) -> str:
    # link richiesto dall’utente
    if not leg or not doc_id:
        return ""
    return f"https://www.senato.it/show-doc?tipodoc=Sindisp&leg={leg}&id={doc_id}"


def fetch_senato_last_48h(
    limit_each: int = 10,
    days: int = 2,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[str]]:
    """
    Returns (ddls, sindacato_ispettivo, warnings)

    DDL dict keys:
      branch, ddl_number, title, date_presentazione, iniziativa, stato, commissione, url

    Sindacato dict keys:
      branch, tipo, numero, titolo, destinatario, proponente, gruppo, stato, url
    """
    warnings: List[str] = []
    start_date = (date.today() - timedelta(days=days)).isoformat()

    # --- DDL (titolo, numero, data presentazione, iniziativa, stato, commissione) ---
    # Nota: la commissione non è garantita (dipende da come/quanto è popolato l’iter).
    q_ddl = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?s ?titolo ?numeroFase ?data ?iniziativa ?stato ?commLabel
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/Ddl> .

  OPTIONAL {{ ?s <http://dati.senato.it/osr/titolo> ?titolo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numeroFase> ?numeroFase . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/descrIniziativa> ?iniziativa . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/statoDdl> ?stato . }}

  # tentativo di risalire alla commissione via Assegnazione (se esiste)
  OPTIONAL {{
    ?ass rdf:type <http://dati.senato.it/osr/Assegnazione> .
    {{
      ?ass <http://dati.senato.it/osr/atto> ?s .
    }} UNION {{
      ?ass <http://dati.senato.it/osr/ddl> ?s .
    }}
    OPTIONAL {{
      ?ass <http://dati.senato.it/osr/commissione> ?comm .
      OPTIONAL {{ ?comm rdfs:label ?commLabel . }}
    }}
  }}

  FILTER(BOUND(?data))
  FILTER(STR(?data) >= "{start_date}")
}}
ORDER BY DESC(?data)
LIMIT {int(limit_each)}
""".strip()

    # --- Sindacato ispettivo (tipo/numero + campi richiesti, quando presenti) ---
    q_sind = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>

SELECT ?s ?tipo ?numero ?data ?titolo ?dest ?prop ?gruppo ?stato ?leg ?url
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/SindacatoIspettivo> .

  OPTIONAL {{ ?s <http://dati.senato.it/osr/tipo> ?tipo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numero> ?numero . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/URLTesto> ?url . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/legislatura> ?leg . }}

  # Titolo (se presente). In alcuni casi potrebbe non esserci.
  OPTIONAL {{ ?s <http://dati.senato.it/osr/titolo> ?titolo . }}

  # A chi è rivolta (se presente): proviamo alcune proprietà plausibili.
  OPTIONAL {{ ?s <http://dati.senato.it/osr/destinatario> ?dest . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/indirizzataA> ?dest . }}

  # Proponente (se presente): spesso descrIniziativa / firmatario / presentatore.
  OPTIONAL {{ ?s <http://dati.senato.it/osr/descrIniziativa> ?prop . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/proponente> ?prop . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/presentatore> ?prop . }}

  # Gruppo parlamentare (se presente)
  OPTIONAL {{ ?s <http://dati.senato.it/osr/gruppo> ?gruppo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/gruppoParlamentare> ?gruppo . }}

  # Stato (se presente): spesso "esito"
  OPTIONAL {{ ?s <http://dati.senato.it/osr/esito> ?stato . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/stato> ?stato . }}

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

    # --- DDL ---
    try:
        res = _request_with_retries(q_ddl)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))
        for r in rows:
            act_uri = r.get("s", "")
            id_fase = _extract_numeric_tail(act_uri)

            titolo = (r.get("titolo", "") or "").strip() or "(senza titolo)"
            numero = (r.get("numeroFase", "") or "").strip()
            data_pres = (r.get("data", "") or "").strip()
            iniziativa = (r.get("iniziativa", "") or "").strip()
            stato = (r.get("stato", "") or "").strip()
            comm = (r.get("commLabel", "") or "").strip()

            ddls.append(
                {
                    "branch": "Senato",
                    "ddl_number": numero,
                    "title": titolo,
                    "date_presentazione": data_pres,
                    "iniziativa": iniziativa,
                    "stato": stato,
                    "commissione": comm,
                    "url": _senato_ddl_link(id_fase) or act_uri,
                }
            )
    except Exception as e:
        warnings.append(f"Query DDL fallita su SPARQL Senato: {type(e).__name__}: {e}")

    # --- Sindacato ispettivo ---
    try:
        res = _request_with_retries(q_sind)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))
        for r in rows:
            act_uri = r.get("s", "")
            tipo = (r.get("tipo", "") or "").strip()
            numero = (r.get("numero", "") or "").strip()
            data_pres = (r.get("data", "") or "").strip()

            titolo = (r.get("titolo", "") or "").strip()
            destinatario = (r.get("dest", "") or "").strip()
            proponente = (r.get("prop", "") or "").strip()
            gruppo = (r.get("gruppo", "") or "").strip()
            stato = (r.get("stato", "") or "").strip()
            leg = (r.get("leg", "") or "").strip()

            # Costruzione link show-doc richiesto
            # Se URLTesto contiene id=... lo usiamo per costruire show-doc.
            url_raw = (r.get("url", "") or "").strip()
            doc_id = ""
            if "id=" in url_raw:
                # es: ...&id=1496581
                doc_id = url_raw.split("id=")[-1].split("&")[0].strip()
            if not doc_id:
                # fallback: prova a prendere coda numerica dell'URI
                doc_id = _extract_numeric_tail(act_uri)

            final_url = _senato_sindisp_link(leg or "19", doc_id) if doc_id else (url_raw or act_uri)

            sind.append(
                {
                    "branch": "Senato",
                    "tipo": tipo,
                    "numero": numero,
                    "titolo": titolo,
                    "destinatario": destinatario,
                    "proponente": proponente,
                    "gruppo": gruppo,
                    "stato": stato,
                    "date_presentazione": data_pres,
                    "url": final_url,
                }
            )
    except Exception as e:
        warnings.append(f"Query Sindacato Ispettivo fallita su SPARQL Senato: {type(e).__name__}: {e}")

    return ddls, sind, warnings
