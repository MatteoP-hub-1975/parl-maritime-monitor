import json
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

SENATO_SPARQL_ENDPOINT = "https://dati.senato.it/sparql"


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
                err_body = err_body.strip().replace("\n", " ")[:500]  # max 500 char
            except Exception:
                err_body = "(impossibile leggere body)"
            last_err = RuntimeError(f"HTTP {he.code} su GET variant {params_dict}. Body: {err_body}")
            continue
        except Exception as e2:
            last_err = e2
            continue

    # If all GET variants fail, raise the last error
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


def fetch_senato_last_48h(
    limit_each: int = 10,
    days: int = 2,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[str]]:
    """
    Returns (ddls, sindacato_ispettivo, warnings)
    """
    warnings: List[str] = []
    start_date = (date.today() - timedelta(days=days)).isoformat()  # YYYY-MM-DD

    q_ddl = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>

SELECT ?s ?label ?numero ?data ?url
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/Ddl> .
  OPTIONAL {{ ?s rdfs:label ?label . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numero> ?numero . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/URLTesto> ?url . }}

  FILTER(BOUND(?data))
  FILTER(STR(?data) >= "{start_date}")
}}
ORDER BY DESC(?data)
LIMIT {int(limit_each)}
""".strip()

    q_sind = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>

SELECT ?s ?label ?numero ?data ?url ?tipo
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/SindacatoIspettivo> .
  OPTIONAL {{ ?s rdfs:label ?label . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numero> ?numero . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/URLTesto> ?url . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/tipo> ?tipo . }}

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

    # DDL
    try:
        res = _request_with_retries(q_ddl)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))
        for r in rows:
            act_uri = r.get("s", "")
            numero = (r.get("numero", "") or "").strip()
            label = (r.get("label", "") or "").strip() or "(senza titolo)"
            url = (r.get("url", "") or "").strip() or act_uri
            data_pres = (r.get("data", "") or "").strip()
            ddls.append(
                {
                    "branch": "Senato",
                    "act_id": f"DDL {numero}".strip(),
                    "title": label,
                    "url": url,
                    "why": "SPARQL Senato (DDL)",
                    "date": data_pres,
                }
            )
    except Exception as e:
        warnings.append(f"Query DDL fallita su SPARQL Senato: {type(e).__name__}: {e}")

    # Sindacato ispettivo
    try:
        res = _request_with_retries(q_sind)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))
        for r in rows:
            act_uri = r.get("s", "")
            numero = (r.get("numero", "") or "").strip()
            label = (r.get("label", "") or "").strip() or "(senza titolo)"
            url = (r.get("url", "") or "").strip() or act_uri
            data_pres = (r.get("data", "") or "").strip()
            tipo = (r.get("tipo", "") or "").strip() or "Sindacato ispettivo"
            sind.append(
                {
                    "branch": "Senato",
                    "act_id": f"{tipo} {numero}".strip(),
                    "title": label,
                    "url": url,
                    "why": "SPARQL Senato (Sindacato ispettivo)",
                    "date": data_pres,
                }
            )
    except Exception as e:
        warnings.append(f"Query Sindacato Ispettivo fallita su SPARQL Senato: {type(e).__name__}: {e}")

    return ddls, sind, warnings
