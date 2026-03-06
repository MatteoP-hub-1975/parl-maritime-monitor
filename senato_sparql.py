import json
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple, Optional

SENATO_SPARQL_ENDPOINT = "https://dati.senato.it/sparql"

# User-Agent "browser-like" (evita di scrivere "GitHub Actions" qui)
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def _strip_sparql_comments(q: str) -> str:
    """Rimuove commenti '#' per ridurre trigger WAF/403."""
    out_lines = []
    for line in q.splitlines():
        # taglia dal primo # in poi
        if "#" in line:
            line = line.split("#", 1)[0]
        out_lines.append(line.rstrip())
    return "\n".join(out_lines).strip()


def _http_error_body(e: urllib.error.HTTPError, max_chars: int = 600) -> str:
    try:
        b = e.read().decode("utf-8", errors="replace")
        b = b.strip().replace("\n", " ")
        return b[:max_chars]
    except Exception:
        return "(impossibile leggere body)"


def _sparql_post_json(query: str, timeout_s: int = 25) -> Dict[str, Any]:
    q = _strip_sparql_comments(query)

    headers = {
        "Accept": "application/sparql-results+json",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        "User-Agent": UA,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://dati.senato.it/sparql",
    }

    # Virtuoso di solito accetta anche "format=json" (aiuta in alcuni casi)
    data = urllib.parse.urlencode(
        {"query": q, "format": "application/sparql-results+json"}
    ).encode("utf-8")

    req = urllib.request.Request(
        SENATO_SPARQL_ENDPOINT,
        data=data,
        headers=headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload)
    except urllib.error.HTTPError as e:
        body = _http_error_body(e)
        raise RuntimeError(f"HTTP {e.code} su POST. Body: {body}") from e


def _request_with_retries(query: str, timeout_s: int = 25, retries: int = 3) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return _sparql_post_json(query=query, timeout_s=timeout_s)
        except Exception as e:
            last_err = e
            # backoff più “dolce” e utile per 403/429
            time.sleep(2 * attempt)
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

    # --- DDL (ok) ---
    q_ddl = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?s ?titolo ?numeroFase ?fase ?data ?stato
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/Ddl> .
  OPTIONAL {{ ?s <http://dati.senato.it/osr/titolo> ?titolo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numeroFase> ?numeroFase . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/fase> ?fase . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/statoDdl> ?stato . }}

  FILTER(BOUND(?data))
  FILTER(STR(?data) >= "{start_date}")
}}
ORDER BY DESC(?data)
LIMIT {int(limit_each)}
""".strip()

    # --- Sindacato ispettivo (QUERY CORTA = niente OPTIONAL “strani”) ---
    q_sind = f"""
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

    # DDL
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

            act_id = f"DDL {numero}" if numero else (fase if fase else "DDL")

            ddls.append(
                {
                    "branch": "Senato",
                    "act_id": act_id,
                    "title": titolo,
                    "url": act_uri,   # lo convertiremo poi al link senato.it come vuoi tu
                    "why": "SPARQL Senato (DDL)",
                    "date": data_pres,
                    "state": stato,
                }
            )
    except Exception as e:
        warnings.append(f"Query DDL fallita su SPARQL Senato: {type(e).__name__}: {e}")

    # pausa breve anti-rate limit / anti-waf
    time.sleep(1.5)

    # Sindacato ispettivo
    try:
        res = _request_with_retries(q_sind)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))
        for r in rows:
            act_uri = (r.get("s", "") or "").strip()
            tipo = (r.get("tipo", "") or "").strip() or "Sindacato ispettivo"
            numero = (r.get("numero", "") or "").strip()
            data_pres = (r.get("data", "") or "").strip()
            url = (r.get("url", "") or "").strip() or act_uri
            leg = (r.get("leg", "") or "").strip()

            sind.append(
                {
                    "branch": "Senato",
                    "act_id": f"{tipo} {numero}".strip(),
                    "title": "(titolo da arricchire su senato.it)",  # step successivo
                    "url": url,
                    "why": "SPARQL Senato (Sindacato ispettivo)",
                    "date": data_pres,
                    "leg": leg,
                    "tipo": tipo,
                    "numero": numero,
                }
            )
    except Exception as e:
        warnings.append(f"Query Sindacato Ispettivo fallita su SPARQL Senato: {type(e).__name__}: {e}")

    return ddls, sind, warnings
