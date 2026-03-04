import json
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple


SENATO_SPARQL_ENDPOINT = "https://dati.senato.it/sparql"


def _post_sparql(
    query: str,
    timeout_s: int = 25,
    retries: int = 3,
    backoff_s: int = 5,
) -> Dict[str, Any]:
    """
    Try POST first; if server returns 403, fallback to GET with encoded query.
    Retries with backoff; raises on final failure.
    """
    headers_common = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "parl-maritime-monitor/0.1 (GitHub Actions)",
    }

    last_err: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            # --- 1) POST ---
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
            last_err = e

            # If forbidden, try GET (many SPARQL endpoints prefer/allow GET)
            if e.code == 403:
                try:
                    params = urllib.parse.urlencode({
                        "query": query,
                        # alcuni endpoint rispettano "format"
                        "format": "application/sparql-results+json",
                    })
                    url = f"{SENATO_SPARQL_ENDPOINT}?{params}"
                    req = urllib.request.Request(
                        url,
                        headers=headers_common,
                        method="GET",
                    )
                    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                        payload = resp.read().decode("utf-8")
                        return json.loads(payload)
                except Exception as e2:
                    last_err = e2

            if attempt < retries:
                time.sleep(backoff_s * attempt)
            else:
                raise last_err

        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_s * attempt)
            else:
                raise last_err

    raise last_err  # type: ignore


def _bindings_to_rows(bindings: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Convert SPARQL JSON bindings to simple dict[str,str].
    """
    rows: List[Dict[str, str]] = []
    for b in bindings:
        row: Dict[str, str] = {}
        for k, v in b.items():
            # v like {"type":"literal","value":"..."} or {"type":"uri","value":"..."}
            row[k] = v.get("value", "")
        rows.append(row)
    return rows


def fetch_senato_last_48h(
    limit_each: int = 10,
    days: int = 2,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[str]]:
    """
    Returns (ddls, sindacato_ispettivo, warnings)

    ddls rows keys: branch, act_id, title, url, why, date
    sindacato rows keys: branch, act_id, title, url, why, date
    """
    warnings: List[str] = []

    start_date = (date.today() - timedelta(days=days)).isoformat()  # YYYY-MM-DD

    # --- Query 1: DDL ---
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
  BIND(xsd:date(?data) AS ?d)
  FILTER(?d >= "{start_date}"^^xsd:date)
}}
ORDER BY DESC(?data)
LIMIT {int(limit_each)}
""".strip()

    # --- Query 2: Sindacato ispettivo (P1: tutti i tipi) ---
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
  BIND(xsd:date(?data) AS ?d)
  FILTER(?d >= "{start_date}"^^xsd:date)

  # P1: includi tutti i tipi che hai trovato
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

    # Run queries with graceful degradation (R1)
    try:
        res = _post_sparql(q_ddl)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))
        for r in rows:
            act_uri = r.get("s", "")
            numero = r.get("numero", "").strip()
            label = r.get("label", "").strip() or "(senza titolo)"
            url = r.get("url", "").strip() or act_uri
            data_pres = r.get("data", "").strip()
            ddls.append({
                "branch": "Senato",
                "act_id": f"DDL {numero}".strip(),
                "title": label,
                "url": url,
                "why": "SPARQL Senato (DDL)",
                "date": data_pres,
            })
    except Exception as e:
        warnings.append(f"Query DDL fallita su SPARQL Senato: {type(e).__name__}: {e}")

    try:
        res = _post_sparql(q_sind)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))
        for r in rows:
            act_uri = r.get("s", "")
            numero = r.get("numero", "").strip()
            label = r.get("label", "").strip() or "(senza titolo)"
            url = r.get("url", "").strip() or act_uri
            data_pres = r.get("data", "").strip()
            tipo = r.get("tipo", "").strip() or "Sindacato ispettivo"
            sind.append({
                "branch": "Senato",
                "act_id": f"{tipo} {numero}".strip(),
                "title": label,
                "url": url,
                "why": "SPARQL Senato (Sindacato ispettivo)",
                "date": data_pres,
            })
    except Exception as e:
        warnings.append(f"Query Sindacato Ispettivo fallita su SPARQL Senato: {type(e).__name__}: {e}")

    return ddls, sind, warnings
