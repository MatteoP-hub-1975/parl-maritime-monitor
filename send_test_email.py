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
"""

WARNINGS: list[str] = []


# =========================
# BASIC UTILS
# =========================

def safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def normalize_dati_url(url: str) -> str:
    url = safe_str(url)
    if not url:
        return ""
    return re.sub(r"^http://dati\.senato\.it", "https://dati.senato.it", url, flags=re.IGNORECASE)


def lodview_html_url(url: str) -> str:
    url = normalize_dati_url(url)
    if not url:
        return ""
    if url.endswith(".html"):
        return url
    if url.startswith("https://dati.senato.it/"):
        return url + ".html"
    return url


def clean_html_text(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def html_to_lines(raw_html: str) -> list[str]:
    s = raw_html
    s = re.sub(r"(?is)<script.*?</script>", " ", s)
    s = re.sub(r"(?is)<style.*?</style>", " ", s)
    s = re.sub(r"(?i)</(p|div|h1|h2|h3|li|tr|td|section|article|br|span|a|title)>", "\n", s)
    s = re.sub(r"(?s)<[^>]+>", " ", s)
    s = html.unescape(s)

    lines = []
    for line in s.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)
    return lines


def parse_possible_dt(value: str) -> datetime | None:
    value = safe_str(value)
    if not value:
        return None

    candidates = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]

    for fmt in candidates:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    # fallback "2026-03-07T00:00:00.000"
    m = re.match(r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})", value)
    if m:
        try:
            dt = datetime.strptime(f"{m.group(1)}T{m.group(2)}", "%Y-%m-%dT%H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    return None


def is_within_last_days(date_str: str, days: int) -> bool:
    dt = parse_possible_dt(date_str)
    if not dt:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return dt >= cutoff


# =========================
# HTTP / SPARQL
# =========================

def http_get(url: str, timeout_s: int = 30, retries: int = 3, backoff_s: float = 2.0) -> str:
    url = normalize_dati_url(url)
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


def url_works(url: str, timeout_s: int = 12) -> bool:
    try:
        url = normalize_dati_url(url)
        req = urllib.request.Request(url, headers=DEFAULT_HEADERS, method="GET")
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return 200 <= getattr(resp, "status", 200) < 400
    except Exception:
        return False


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
# URL HELPERS
# =========================

def build_showdoc_url(docid: str, leg: str = "19") -> str:
    docid = safe_str(docid)
    leg = safe_str(leg) or "19"
    if not docid:
        return ""
    return f"https://www.senato.it/show-doc?id={docid}&leg={leg}&tipodoc=Sindisp"


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
# PARSERS
# =========================

def parse_sindisp_lodview(raw_html: str, source_url: str) -> dict[str, str]:
    out = {
        "lodview_url": safe_str(source_url),
        "showdoc_url": "",
        "docid": "",
        "leg": "19",
        "iniziativa_url": "",
        "tipo": "",
        "numero": "",
        "data_presentazione": "",
    }

    text = html.unescape(raw_html)

    m = re.search(
        r"https?://www\.senato\.it/loc/link\.asp\?tipodoc=sindisp&leg=(\d+)&id=(\d+)",
        text,
        flags=re.IGNORECASE,
    )
    if m:
        out["leg"] = m.group(1)
        out["docid"] = m.group(2)
        out["showdoc_url"] = build_showdoc_url(out["docid"], out["leg"])

    m = re.search(
        r'href="(https?://dati\.senato\.it/iniziativa/[^"]+)"',
        text,
        flags=re.IGNORECASE,
    )
    if m:
        out["iniziativa_url"] = normalize_dati_url(m.group(1))

    lines = html_to_lines(raw_html)

    for ln in lines:
        if not out["numero"]:
            m = re.search(r"\b([2345]-\d{5})\b", ln)
            if m:
                out["numero"] = m.group(1)

        if not out["data_presentazione"]:
            m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", ln)
            if m:
                out["data_presentazione"] = m.group(1)

        if not out["tipo"]:
            lower_ln = ln.lower()
            if "interrogazione con richiesta di risposta scritta" in lower_ln:
                out["tipo"] = "Interrogazione con richiesta di risposta scritta"
            elif "interrogazione a risposta scritta" in lower_ln:
                out["tipo"] = "Interrogazione a risposta scritta"
            elif "interrogazione" in lower_ln:
                out["tipo"] = "Interrogazione"
            elif "interpellanza" in lower_ln:
                out["tipo"] = "Interpellanza"
            elif "mozione" in lower_ln:
                out["tipo"] = "Mozione"
            elif "risoluzione" in lower_ln:
                out["tipo"] = "Risoluzione"

    return out


def parse_iniziativa_lodview(raw_html: str) -> dict[str, str]:
    out = {
        "proponente": "",
        "senatore_url": "",
    }

    text = html.unescape(raw_html)

    m = re.search(
        r'href="(https?://dati\.senato\.it/senatore/\d+)"[^>]*>\s*([^<]+?)\s*</a>',
        text,
        flags=re.IGNORECASE,
    )
    if m:
        out["senatore_url"] = normalize_dati_url(m.group(1))
        out["proponente"] = clean_html_text(m.group(2))
        return out

    m = re.search(
        r'href="(/senatore/\d+)"[^>]*>\s*([^<]+?)\s*</a>',
        text,
        flags=re.IGNORECASE,
    )
    if m:
        out["senatore_url"] = normalize_dati_url("https://dati.senato.it" + m.group(1))
        out["proponente"] = clean_html_text(m.group(2))
        return out

    lines = html_to_lines(raw_html)
    for i, ln in enumerate(lines):
        low = ln.lower()
        if "presentatore" in low or "primo firmatario" in low:
            if i + 1 < len(lines):
                nxt = lines[i + 1].strip()
                if nxt and len(nxt) < 200:
                    out["proponente"] = nxt
                    break

    return out


def parse_senatore_lodview(raw_html: str) -> dict[str, str]:
    out = {
        "gruppo": "",
        "gruppo_url": "",
    }

    text = html.unescape(raw_html)

    m = re.search(
        r'href="(https?://dati\.senato\.it/gruppo/\d+)"[^>]*>\s*([^<]+?)\s*</a>',
        text,
        flags=re.IGNORECASE,
    )
    if m:
        out["gruppo_url"] = normalize_dati_url(m.group(1))
        out["gruppo"] = clean_html_text(m.group(2))
        return out

    m = re.search(
        r'href="(/gruppo/\d+)"[^>]*>\s*([^<]+?)\s*</a>',
        text,
        flags=re.IGNORECASE,
    )
    if m:
        out["gruppo_url"] = normalize_dati_url("https://dati.senato.it" + m.group(1))
        out["gruppo"] = clean_html_text(m.group(2))
        return out

    return out


def parse_gruppo_lodview(raw_html: str) -> dict[str, str]:
    out = {"gruppo": ""}

    text = html.unescape(raw_html)

    m = re.search(r"<title>(.*?)</title>", text, flags=re.IGNORECASE | re.DOTALL)
    if m:
        title = clean_html_text(m.group(1))
        title = re.sub(r"\s*-\s*dati\.senato\.it\s*$", "", title, flags=re.IGNORECASE).strip()
        if title:
            out["gruppo"] = title
            return out

    lines = html_to_lines(raw_html)
    for ln in lines:
        clean = ln.strip()
        if not clean:
            continue
        if len(clean) < 3:
            continue
        if clean.lower().startswith("http"):
            continue
        if "dati.senato.it" in clean.lower():
            continue
        out["gruppo"] = clean
        break

    return out


def parse_showdoc(raw_html: str) -> dict[str, str]:
    out = {
        "destinatario": "",
        "stato": "",
        "numero": "",
    }

    lines = html_to_lines(raw_html)

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
                out["destinatario"] = parts[1].strip().rstrip(".")
            break

    return out


# =========================
# DDL
# =========================

def query_ddls_recent(limit_each: int) -> list[dict[str, str]]:
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
}}
ORDER BY DESC(?dataPresentazione)
LIMIT {int(limit_each)}
"""
    try:
        return sparql_select(query)
    except Exception as e:
        WARNINGS.append(f"Query DDL fallita: {type(e).__name__}: {e}")
        return []


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


def fetch_ddls_last_days(limit_each: int, days: int) -> list[dict[str, str]]:
    rows = query_ddls_recent(limit_each=max(limit_each * 5, 300))

    items: list[dict[str, str]] = []
    for r in rows:
        data_presentazione = safe_str(r.get("dataPresentazione"))
        if not is_within_last_days(data_presentazione, days):
            continue

        idfase = safe_str(r.get("idFase"))
        items.append({
            "branch": "Senato",
            "ddl_number": safe_str(r.get("numero")),
            "title": safe_str(r.get("titolo")),
            "date_presentazione": data_presentazione,
            "iniziativa": safe_str(r.get("iniziativaLabel")),
            "stato": safe_str(r.get("statoLabel")),
            "commissione": safe_str(r.get("commissioneLabel")),
            "url": canonical_ddl_url(safe_str(r.get("url")), fallback_idfase=idfase),
        })

    items = dedupe_ddls(items)
    return items[:limit_each]


# =========================
# SINDISP
# =========================

def query_sindisp_recent(limit_each: int) -> list[dict[str, str]]:
    query = f"""
{PREFIXES}
SELECT DISTINCT
    ?atto
    ?numero
    ?tipoLabel
    ?dataPresentazione
WHERE {{
    ?atto rdf:type osr:SindacatoIspettivo .
    OPTIONAL {{ ?atto osr:numero ?numero . }}
    OPTIONAL {{ ?atto osr:dataPresentazione ?dataPresentazione . }}
    OPTIONAL {{
        ?atto osr:tipo ?tipo .
        ?tipo rdfs:label ?tipoLabel .
    }}
}}
ORDER BY DESC(?dataPresentazione)
LIMIT {int(limit_each)}
"""
    try:
        return sparql_select(query)
    except Exception as e:
        WARNINGS.append(f"Query Sindisp base fallita: {type(e).__name__}: {e}")
        return []


def best_public_link(showdoc_url: str, lodview_url: str) -> tuple[str, str]:
    showdoc_url = safe_str(showdoc_url)
    lodview_url = safe_str(lodview_url)

    if showdoc_url and url_works(showdoc_url):
        return showdoc_url, lodview_url

    if lodview_url and url_works(lodview_url):
        return lodview_url, showdoc_url

    if lodview_url:
        return lodview_url, showdoc_url
    return showdoc_url, lodview_url


def enrich_single_sindisp(base_item: dict[str, str]) -> dict[str, str]:
    out = dict(base_item)

    atto_uri = normalize_dati_url(base_item.get("atto"))
    lodview_url = lodview_html_url(atto_uri)

    raw_lodview = ""
    parsed_lod = {}

    if lodview_url:
        try:
            raw_lodview = http_get(lodview_url, timeout_s=25, retries=3, backoff_s=2.0)
            parsed_lod = parse_sindisp_lodview(raw_lodview, lodview_url)
        except Exception as e:
            WARNINGS.append(
                f"LodView Sindisp non raggiungibile per {safe_str(base_item.get('numero')) or '-'}: {type(e).__name__}: {e}"
            )

    tipo = safe_str(parsed_lod.get("tipo")) or safe_str(base_item.get("tipo")) or "Sindacato ispettivo"
    numero = safe_str(base_item.get("numero")) or safe_str(parsed_lod.get("numero"))
    data_presentazione = safe_str(base_item.get("data_presentazione")) or safe_str(parsed_lod.get("data_presentazione"))

    showdoc_url = safe_str(parsed_lod.get("showdoc_url"))
    iniziativa_url = lodview_html_url(parsed_lod.get("iniziativa_url"))

    proponente = ""
    gruppo = ""
    destinatario = ""
    stato = ""

    if iniziativa_url:
        try:
            raw_iniziativa = http_get(iniziativa_url, timeout_s=25, retries=3, backoff_s=2.0)
            parsed_iniziativa = parse_iniziativa_lodview(raw_iniziativa)
            proponente = safe_str(parsed_iniziativa.get("proponente"))
            senatore_url = lodview_html_url(parsed_iniziativa.get("senatore_url"))

            if senatore_url:
                try:
                    raw_senatore = http_get(senatore_url, timeout_s=25, retries=3, backoff_s=2.0)
                    parsed_senatore = parse_senatore_lodview(raw_senatore)
                    gruppo = safe_str(parsed_senatore.get("gruppo"))
                    gruppo_url = lodview_html_url(parsed_senatore.get("gruppo_url"))

                    if gruppo_url and not gruppo:
                        try:
                            raw_gruppo = http_get(gruppo_url, timeout_s=25, retries=3, backoff_s=2.0)
                            parsed_gruppo = parse_gruppo_lodview(raw_gruppo)
                            gruppo = safe_str(parsed_gruppo.get("gruppo"))
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

    if showdoc_url:
        try:
            raw_showdoc = http_get(showdoc_url, timeout_s=25, retries=2, backoff_s=2.0)
            parsed_showdoc = parse_showdoc(raw_showdoc)
            destinatario = safe_str(parsed_showdoc.get("destinatario"))
            stato = safe_str(parsed_showdoc.get("stato"))
            if not numero:
                numero = safe_str(parsed_showdoc.get("numero"))
        except Exception:
            pass

    link, link_fallback = best_public_link(showdoc_url, lodview_url)

    out.update({
        "branch": "Senato",
        "tipo": tipo,
        "numero": numero,
        "data_presentazione": data_presentazione,
        "proponente": proponente,
        "gruppo": gruppo,
        "destinatario": destinatario,
        "stato": stato,
        "url": link,
        "url_fallback": link_fallback,
        "showdoc_url": showdoc_url,
        "lodview_url": lodview_url,
    })

    return out


def dedupe_sindisp(items: list[dict[str, str]]) -> list[dict[str, str]]:
    def score(it: dict[str, str]) -> int:
        fields = [
            "tipo",
            "numero",
            "proponente",
            "gruppo",
            "destinatario",
            "stato",
            "url",
            "url_fallback",
        ]
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
        key=lambda x: (safe_str(x.get("data_presentazione")), safe_str(x.get("numero"))),
        reverse=True,
    )
    return out


def fetch_sindisp_last_days(limit_each: int, days: int) -> list[dict[str, str]]:
    rows = query_sindisp_recent(limit_each=max(limit_each * 5, 300))

    base_items: list[dict[str, str]] = []
    for r in rows:
        data_presentazione = safe_str(r.get("dataPresentazione"))
        if not is_within_last_days(data_presentazione, days):
            continue

        base_items.append({
            "atto": normalize_dati_url(safe_str(r.get("atto"))),
            "branch": "Senato",
            "tipo": safe_str(r.get("tipoLabel")),
            "numero": safe_str(r.get("numero")),
            "data_presentazione": data_presentazione,
        })

    out: list[dict[str, str]] = []
    for it in base_items:
        try:
            out.append(enrich_single_sindisp(it))
            time.sleep(0.35)
        except Exception as e:
            WARNINGS.append(
                f"Enrichment Sindisp fallito per {safe_str(it.get('numero')) or '-'}: {type(e).__name__}: {e}"
            )
            fallback_lodview = lodview_html_url(it.get("atto"))
            out.append({
                "branch": "Senato",
                "tipo": safe_str(it.get("tipo")) or "Sindacato ispettivo",
                "numero": safe_str(it.get("numero")),
                "data_presentazione": safe_str(it.get("data_presentazione")),
                "proponente": "",
                "gruppo": "",
                "destinatario": "",
                "stato": "",
                "url": fallback_lodview,
                "url_fallback": "",
                "showdoc_url": "",
                "lodview_url": fallback_lodview,
            })

    deduped = dedupe_sindisp(out)
    return deduped[:limit_each]


# =========================
# PUBLIC API
# =========================

def fetch_senato_last_48h(limit_each: int = 200, days: int = 2) -> tuple[list[dict[str, str]], list[dict[str, str]], list[str]]:
    WARNINGS.clear()

    ddls = fetch_ddls_last_days(limit_each=limit_each, days=days)
    sind = fetch_sindisp_last_days(limit_each=limit_each, days=days)

    WARNINGS.append(f"DEBUG DDL trovati dopo filtro Python: {len(ddls)}")
    WARNINGS.append(f"DEBUG Sindisp trovati dopo filtro Python: {len(sind)}")

    return ddls, sind, list(WARNINGS)
