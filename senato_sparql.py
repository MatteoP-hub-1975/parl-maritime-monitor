import json
import re
import html
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

SPARQL_ENDPOINT = "https://dati.senato.it/sparql"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/sparql-results+json, application/json, text/html;q=0.9, */*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://dati.senato.it/",
    "Connection": "close",
}

PREFIXES = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX osr: <http://dati.senato.it/osr/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX dcterms: <http://purl.org/dc/terms/>
"""

WARNINGS: list[str] = []


# =========================
# BASIC UTILS
# =========================

def safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def iso_cutoff(days: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def html_to_lines(raw_html: str) -> list[str]:
    s = raw_html
    s = re.sub(r"(?is)<script.*?</script>", " ", s)
    s = re.sub(r"(?is)<style.*?</style>", " ", s)
    s = re.sub(r"(?i)</(p|div|h1|h2|h3|li|tr|td|section|article|br|span)>", "\n", s)
    s = re.sub(r"(?s)<[^>]+>", " ", s)
    s = html.unescape(s)

    lines = []
    for line in s.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)
    return lines


# =========================
# HTTP / SPARQL
# =========================

def http_get(url: str, timeout_s: int = 30, retries: int = 3, backoff_s: float = 2.0) -> str:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=DEFAULT_HEADERS, method="GET")
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_s * attempt)
            else:
                raise last_err


def sparql_select(query: str, timeout_s: int = 90) -> list[dict[str, str]]:
    params = urllib.parse.urlencode({
        "query": query,
        "format": "application/sparql-results+json",
    })
    url = f"{SPARQL_ENDPOINT}?{params}"

    raw = http_get(url, timeout_s=timeout_s, retries=3, backoff_s=3.0)
    payload = json.loads(raw)

    rows: list[dict[str, str]] = []
    for b in payload.get("results", {}).get("bindings", []):
        row: dict[str, str] = {}
        for k, v in b.items():
            row[k] = v.get("value", "")
        rows.append(row)
    return rows


# =========================
# URL NORMALIZATION
# =========================

def extract_docid_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"[?&]id=(\d+)", url)
    return m.group(1) if m else ""


def extract_leg_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"[?&]leg=(\d+)", url)
    return m.group(1) if m else ""


def extract_lodview_urltesto(lodview_url: str) -> tuple[str, str]:
    if not lodview_url:
        return "", ""

    url = lodview_url
    if not url.endswith(".html"):
        url += ".html"

    try:
        raw = http_get(url, timeout_s=25)
    except Exception:
        return "", ""

    text = html.unescape(raw)

    m = re.search(
        r"https?://www\.senato\.it/loc/link\.asp\?tipodoc=sindisp&leg=(\d+)&id=(\d+)",
        text,
        flags=re.IGNORECASE,
    )
    if m:
        return m.group(1), m.group(2)

    return "", ""


def canonical_sindisp_url(raw_url: str, default_leg: str = "19") -> str:
    raw_url = safe_str(raw_url)
    if not raw_url:
        return ""

    if "show-doc" in raw_url:
        docid = extract_docid_from_url(raw_url)
        leg = extract_leg_from_url(raw_url) or default_leg
        if docid:
            return f"https://www.senato.it/show-doc?id={docid}&leg={leg}&tipodoc=Sindisp"
        return raw_url

    docid = extract_docid_from_url(raw_url)
    leg = extract_leg_from_url(raw_url) or default_leg
    if docid:
        return f"https://www.senato.it/show-doc?id={docid}&leg={leg}&tipodoc=Sindisp"

    if "dati.senato.it/sindacatoispettivo/" in raw_url:
        leg2, docid2 = extract_lodview_urltesto(raw_url)
        if docid2:
            return f"https://www.senato.it/show-doc?id={docid2}&leg={leg2 or default_leg}&tipodoc=Sindisp"

    return raw_url


def canonical_ddl_url(raw_url: str, fallback_idfase: str = "") -> str:
    raw_url = safe_str(raw_url)
    if "scheda-ddl?did=" in raw_url:
        return raw_url

    m = re.search(r"[?&]did=(\d+)", raw_url)
    if m:
        return f"https://www.senato.it/leggi-e-documenti/disegni-di-legge/scheda-ddl?did={m.group(1)}"

    if fallback_idfase:
        return f"https://www.senato.it/leggi-e-documenti/disegni-di-legge/scheda-ddl?did={fallback_idfase}"

    return raw_url


# =========================
# HTML ENRICHMENT: SINDISP
# =========================

def parse_sindisp_showdoc(showdoc_url: str) -> dict[str, str]:
    out = {
        "destinatario": "",
        "proponente": "",
        "stato": "",
        "numero": "",
    }

    if not showdoc_url or "show-doc" not in showdoc_url:
        return out

    try:
        raw = http_get(showdoc_url, timeout_s=25)
        lines = html_to_lines(raw)
    except Exception:
        return out

    for ln in lines:
        m = re.search(r"\bAtto n\.\s*([0-9\-\/]+)", ln, re.IGNORECASE)
        if m:
            out["numero"] = m.group(1).strip()
            break

    status_prefixes = (
        "Svolto",
        "Svolto question time",
        "Trasformato",
        "Assegnato",
        "Concluso",
        "In corso",
        "Illustrato",
        "Ritirato",
        "Decaduto",
        "Risposta",
        "Discussa",
        "Pubblicato",
    )
    for ln in lines:
        if ln.startswith(status_prefixes):
            out["stato"] = ln
            break

    for ln in lines:
        if " - Al " in ln or " - Ai " in ln or " - Alla " in ln or " - Alle " in ln or " - Al Presidente" in ln:
            parts = [p.strip(" -") for p in ln.split(" - ") if p.strip(" -")]
            if len(parts) >= 2:
                out["proponente"] = parts[0].strip()
                out["destinatario"] = parts[1].strip().rstrip(".")
            break

    return out


# =========================
# DDL
# =========================

def query_ddls_last_days(limit_each: int, days: int) -> list[dict[str, str]]:
    cutoff = iso_cutoff(days)

    query = f"""
{PREFIXES}
SELECT DISTINCT
    ?atto
    ?numero
    ?titolo
    ?dataPresentazione
    ?iniziativaLabel
    ?statoLabel
    ?commissioneLabel
    ?idFase
    ?url
WHERE {{
    ?atto rdf:type osr:DDL .

    OPTIONAL {{ ?atto dc:title ?titolo . }}
    OPTIONAL {{ ?atto osr:numero ?numero . }}
    OPTIONAL {{ ?atto osr:dataPresentazione ?dataPresentazione . }}
    OPTIONAL {{
        ?atto osr:stato ?stato .
        ?stato rdfs:label ?statoLabel .
    }}
    OPTIONAL {{
        ?atto osr:commissione ?commissione .
        ?commissione rdfs:label ?commissioneLabel .
    }}
    OPTIONAL {{
        ?atto osr:iniziativa ?iniziativa .
        ?iniziativa rdfs:label ?iniziativaLabel .
    }}
    OPTIONAL {{ ?atto osr:idFase ?idFase . }}
    OPTIONAL {{ ?atto osr:url ?url . }}

    FILTER(BOUND(?dataPresentazione))
    FILTER(xsd:dateTime(?dataPresentazione) >= xsd:dateTime("{cutoff}"))
}}
ORDER BY DESC(?dataPresentazione)
LIMIT {int(limit_each)}
"""
    try:
        rows = sparql_select(query)
    except Exception as e:
        WARNINGS.append(f"Query DDL fallita: {type(e).__name__}: {e}")
        return []

    items: list[dict[str, str]] = []
    for r in rows:
        idfase = safe_str(r.get("idFase"))
        url = canonical_ddl_url(safe_str(r.get("url")), fallback_idfase=idfase)
        items.append({
            "branch": "Senato",
            "ddl_number": safe_str(r.get("numero")),
            "title": safe_str(r.get("titolo")),
            "date_presentazione": safe_str(r.get("dataPresentazione")),
            "iniziativa": safe_str(r.get("iniziativaLabel")),
            "stato": safe_str(r.get("statoLabel")),
            "commissione": safe_str(r.get("commissioneLabel")),
            "url": url,
        })

    return dedupe_ddls(items)


def dedupe_ddls(items: list[dict[str, str]]) -> list[dict[str, str]]:
    seen = set()
    out = []
    for it in items:
        key = (
            safe_str(it.get("ddl_number")).lower(),
            safe_str(it.get("url")).lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


# =========================
# SINDISP
# =========================

def query_sindisp_base(limit_each: int, days: int) -> list[dict[str, str]]:
    cutoff = iso_cutoff(days)

    query = f"""
{PREFIXES}
SELECT DISTINCT
    ?atto
    ?numero
    ?tipoLabel
    ?dataPresentazione
    ?urlTesto
    ?rawUrl
WHERE {{
    ?atto rdf:type osr:SindacatoIspettivo .

    OPTIONAL {{ ?atto osr:numero ?numero . }}
    OPTIONAL {{ ?atto osr:dataPresentazione ?dataPresentazione . }}
    OPTIONAL {{
        ?atto osr:tipo ?tipo .
        ?tipo rdfs:label ?tipoLabel .
    }}
    OPTIONAL {{ ?atto osr:URLTesto ?urlTesto . }}
    OPTIONAL {{ ?atto osr:url ?rawUrl . }}

    FILTER(BOUND(?dataPresentazione))
    FILTER(xsd:dateTime(?dataPresentazione) >= xsd:dateTime("{cutoff}"))
}}
ORDER BY DESC(?dataPresentazione)
LIMIT {int(limit_each)}
"""
    try:
        return sparql_select(query)
    except Exception as e:
        WARNINGS.append(f"Query Sindisp base fallita: {type(e).__name__}: {e}")
        return []


def query_sindisp_presenters(limit_each: int, days: int) -> list[dict[str, str]]:
    cutoff = iso_cutoff(days)

    query = f"""
{PREFIXES}
SELECT DISTINCT
    ?atto
    ?numero
    ?presentatoreLabel
    ?gruppoLabel
WHERE {{
    ?atto rdf:type osr:SindacatoIspettivo .
    ?atto osr:dataPresentazione ?dataPresentazione .
    FILTER(xsd:dateTime(?dataPresentazione) >= xsd:dateTime("{cutoff}"))

    OPTIONAL {{ ?atto osr:numero ?numero . }}

    OPTIONAL {{
        ?atto osr:iniziativa ?iniziativa .

        OPTIONAL {{
            ?iniziativa osr:presentatore ?presentatore .
            ?presentatore rdfs:label ?presentatoreLabel .
        }}

        OPTIONAL {{
            ?iniziativa osr:primoFirmatario ?primoFirmatario .
            ?primoFirmatario rdfs:label ?presentatoreLabel .
        }}

        OPTIONAL {{
            ?iniziativa osr:senatore ?senatore .
            ?senatore rdfs:label ?presentatoreLabel .
        }}

        OPTIONAL {{
            ?iniziativa osr:presentatore ?p2 .
            ?p2 osr:gruppo ?gruppo .
            ?gruppo rdfs:label ?gruppoLabel .
        }}

        OPTIONAL {{
            ?iniziativa osr:primoFirmatario ?pf2 .
            ?pf2 osr:gruppo ?gruppo2 .
            ?gruppo2 rdfs:label ?gruppoLabel .
        }}

        OPTIONAL {{
            ?iniziativa osr:senatore ?s2 .
            ?s2 osr:gruppo ?gruppo3 .
            ?gruppo3 rdfs:label ?gruppoLabel .
        }}
    }}
}}
LIMIT {int(limit_each) * 8}
"""
    try:
        return sparql_select(query)
    except Exception as e:
        WARNINGS.append(f"Query Sindisp presentatori/gruppi fallita: {type(e).__name__}: {e}")
        return []


def merge_sindisp_rows(
    base_rows: list[dict[str, str]],
    presenter_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    by_key: dict[str, dict[str, str]] = {}

    def row_key(row: dict[str, str]) -> str:
        atto = safe_str(row.get("atto"))
        numero = safe_str(row.get("numero"))
        if atto:
            return f"atto::{atto}"
        return f"numero::{numero}"

    for r in base_rows:
        raw_url = safe_str(r.get("urlTesto") or r.get("rawUrl"))
        by_key[row_key(r)] = {
            "branch": "Senato",
            "tipo": safe_str(r.get("tipoLabel")) or "Sindacato ispettivo",
            "numero": safe_str(r.get("numero")),
            "data_presentazione": safe_str(r.get("dataPresentazione")),
            "proponente": "",
            "gruppo": "",
            "destinatario": "",
            "stato": "",
            "url": canonical_sindisp_url(raw_url, default_leg="19"),
        }

    for r in presenter_rows:
        key = row_key(r)
        if key not in by_key:
            by_key[key] = {
                "branch": "Senato",
                "tipo": "Sindacato ispettivo",
                "numero": safe_str(r.get("numero")),
                "data_presentazione": "",
                "proponente": "",
                "gruppo": "",
                "destinatario": "",
                "stato": "",
                "url": "",
            }

        existing = by_key[key]

        p = safe_str(r.get("presentatoreLabel"))
        g = safe_str(r.get("gruppoLabel"))

        if p:
            if not existing["proponente"]:
                existing["proponente"] = p
            else:
                existing_names = {x.strip() for x in existing["proponente"].split(";") if x.strip()}
                if p not in existing_names:
                    existing["proponente"] = existing["proponente"] + "; " + p

        if g and not existing["gruppo"]:
            existing["gruppo"] = g

    return list(by_key.values())


def enrich_sindisp_items(items: list[dict[str, str]], sleep_s: float = 0.35) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []

    for it in items:
        enriched = dict(it)
        url = canonical_sindisp_url(safe_str(enriched.get("url")), default_leg="19")
        enriched["url"] = url

        try:
            details = parse_sindisp_showdoc(url)
        except Exception as e:
            WARNINGS.append(
                f"Enrichment Sindisp fallito per {safe_str(it.get('numero')) or '-'}: {type(e).__name__}: {e}"
            )
            details = {}

        if not enriched.get("numero") and details.get("numero"):
            enriched["numero"] = details["numero"]

        if not enriched.get("destinatario") and details.get("destinatario"):
            enriched["destinatario"] = details["destinatario"]

        if not enriched.get("proponente") and details.get("proponente"):
            enriched["proponente"] = details["proponente"]

        if not enriched.get("stato") and details.get("stato"):
            enriched["stato"] = details["stato"]

        out.append(enriched)
        time.sleep(sleep_s)

    return out


def dedupe_sindisp(items: list[dict[str, str]]) -> list[dict[str, str]]:
    def score(it: dict[str, str]) -> int:
        fields = ["tipo", "numero", "proponente", "gruppo", "destinatario", "stato", "url"]
        return sum(1 for f in fields if safe_str(it.get(f)))

    best_by_key: dict[tuple[str, str], dict[str, str]] = {}

    for it in items:
        key = (
            safe_str(it.get("tipo")).lower(),
            safe_str(it.get("numero")).lower(),
        )
        if key not in best_by_key or score(it) > score(best_by_key[key]):
            best_by_key[key] = it

    out = list(best_by_key.values())
    out.sort(
        key=lambda it: (safe_str(it.get("data_presentazione")), safe_str(it.get("numero"))),
        reverse=True,
    )
    return out


def fetch_sindisp_last_days(limit_each: int, days: int) -> list[dict[str, str]]:
    base_rows = query_sindisp_base(limit_each=limit_each, days=days)
    presenter_rows = query_sindisp_presenters(limit_each=limit_each, days=days)

    merged = merge_sindisp_rows(base_rows, presenter_rows)
    enriched = enrich_sindisp_items(merged)
    deduped = dedupe_sindisp(enriched)
    return deduped[:limit_each]


# =========================
# PUBLIC API
# =========================

def fetch_senato_last_48h(limit_each: int = 200, days: int = 2) -> tuple[list[dict[str, str]], list[dict[str, str]], list[str]]:
    WARNINGS.clear()

    ddls = query_ddls_last_days(limit_each=limit_each, days=days)
    sind = fetch_sindisp_last_days(limit_each=limit_each, days=days)

    return ddls, sind, list(WARNINGS)
