import datetime as dt
import html
import logging
import re
import urllib.parse
from typing import Dict, Iterable, List, Optional

import requests

SPARQL_ENDPOINT = "https://dati.senato.it/sparql"
USER_AGENT = "monitor-parlamento-confitarma/1.0"
TIMEOUT = 40

PREFIXES = """
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX dc:   <http://purl.org/dc/elements/1.1/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX osr:  <http://dati.senato.it/osr/>
PREFIX ocd:  <http://dati.camera.it/ocd/>
""".strip()

LOG = logging.getLogger(__name__)
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def _value(binding: Dict, key: str, default: str = "") -> str:
    try:
        return binding.get(key, {}).get("value", default) or default
    except Exception:
        return default


def _iso_date_days_ago(days_back: int) -> str:
    return (dt.date.today() - dt.timedelta(days=days_back)).isoformat()


def _parse_iso_date(value: str) -> Optional[dt.date]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dZ", "%Y-%m-%d+00:00"):
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _get(url: str, **kwargs) -> requests.Response:
    last_exc = None
    for _ in range(3):
        try:
            resp = SESSION.get(url, timeout=TIMEOUT, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
    raise last_exc


def run_sparql(query: str) -> List[Dict]:
    payload = {
        "default-graph-uri": "",
        "query": f"{PREFIXES}\n{query}",
        "format": "application/sparql-results+json",
        "timeout": "0",
    }

    headers = {
        "Accept": "application/sparql-results+json, application/json;q=0.9, */*;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://dati.senato.it",
        "Referer": "https://dati.senato.it/sparql",
    }

    resp = SESSION.post(
        SPARQL_ENDPOINT,
        data=payload,
        headers=headers,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()

    payload = resp.json()
    return payload.get("results", {}).get("bindings", [])


def _compact_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _uniq(values: Iterable[str]) -> List[str]:
    out, seen = [], set()
    for value in values:
        value = _compact_spaces(value)
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _join(values: Iterable[str], sep: str = " | ", fallback: str = "-") -> str:
    cleaned = _uniq(values)
    return sep.join(cleaned) if cleaned else fallback


def _strip_tags_to_lines(html_text: str) -> List[str]:
    html_text = re.sub(r"(?is)<script.*?>.*?</script>", " ", html_text)
    html_text = re.sub(r"(?is)<style.*?>.*?</style>", " ", html_text)
    html_text = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", html_text)
    html_text = re.sub(
        r"(?i)</\s*(p|div|section|article|li|ul|ol|h1|h2|h3|h4|h5|h6|tr|table)\s*>",
        "\n",
        html_text,
    )
    text = re.sub(r"(?s)<[^>]+>", " ", html_text)
    text = html.unescape(text).replace("\xa0", " ")
    raw_lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()]
    return [line for line in raw_lines if line]


def _looks_like_presenters_line(line: str) -> bool:
    if re.search(r"\s-\sA(?:i|l|ll[aeo]?|gli)\b", line):
        return True
    letters = re.sub(r"[^A-ZÀ-ÖØ-Ý]", "", line.upper())
    ratio = (len(letters) / max(len(line), 1))
    return ratio > 0.45 and ("," in line or len(line.split()) <= 8)


def _split_presenters_target(line: str):
    m = re.match(
        r"^(?P<prop>.+?)\s-\s(?P<target>A(?:i|l|ll[aeo]?|gli)\s.+?)\s-\s*$",
        line,
        flags=re.IGNORECASE,
    )
    if m:
        return _compact_spaces(m.group("prop")), _compact_spaces(m.group("target"))
    return _compact_spaces(line.strip(" -")), "-"


def resolve_public_url(url: str, tipodoc_hint: str = "Sindisp") -> str:
    if not url:
        return ""
    try:
        resp = _get(url, allow_redirects=True)
    except Exception:
        return url

    final_url = resp.url or url
    if "show-doc" in final_url:
        return final_url

    patterns = [
        rf'https?://www\.senato\.it/show-doc[^"\']*tipodoc={re.escape(tipodoc_hint)}[^"\']*',
        rf'href="([^"]*show-doc[^"]*tipodoc={re.escape(tipodoc_hint)}[^"]*)"',
        rf"href='([^']*show-doc[^']*tipodoc={re.escape(tipodoc_hint)}[^']*)'",
    ]
    for pattern in patterns:
        m = re.search(pattern, resp.text, flags=re.IGNORECASE)
        if m:
            candidate = m.group(1) if m.groups() else m.group(0)
            return urllib.parse.urljoin(final_url, html.unescape(candidate))

    return final_url


def enrich_sindisp_from_page(url: str) -> Dict[str, str]:
    if not url:
        return {}

    try:
        resp = _get(url, allow_redirects=True)
    except Exception as exc:
        LOG.warning("Impossibile leggere la pagina atto %s: %s", url, exc)
        return {}

    lines = _strip_tags_to_lines(resp.text)
    if not lines:
        return {}

    pub_idx = next((i for i, line in enumerate(lines) if line.startswith("Pubblicato il ")), None)
    if pub_idx is None:
        pub_idx = next((i for i, line in enumerate(lines) if re.match(r"^Pubblicato il \d", line)), None)

    presenter_idx = None
    if pub_idx is not None:
        for i in range(pub_idx + 1, min(pub_idx + 8, len(lines))):
            if _looks_like_presenters_line(lines[i]):
                presenter_idx = i
                break

    presenters = "-"
    target = "-"
    status = "-"
    body = ""
    presented_on = ""

    if pub_idx is not None:
        presented_on = lines[pub_idx]

    if presenter_idx is not None:
        presenters, target = _split_presenters_target(lines[presenter_idx])
        status_lines = [
            line for line in lines[pub_idx + 1 : presenter_idx]
            if not line.startswith("Atto n.") and not line.startswith("## ")
        ]
        status = _join(status_lines, sep=" | ", fallback="-")

        body_lines = []
        for line in lines[presenter_idx + 1 :]:
            if line.startswith("## Servizio - Bottom Footer"):
                break
            body_lines.append(line)
        body = _compact_spaces(" ".join(body_lines))

    return {
        "public_link": resp.url or url,
        "presented_line": presented_on,
        "status": status,
        "proponents": presenters,
        "target": target,
        "page_text": body,
    }


def _act_groups_query(act_uri: str, act_date_iso: str) -> str:
    return f"""
SELECT DISTINCT ?senatore ?nomeCompleto ?gruppoTitolo WHERE {{
  BIND(<{act_uri}> AS ?atto)
  ?atto osr:iniziativa ?iniziativa .
  ?iniziativa osr:senatore ?senatore .

  OPTIONAL {{ ?senatore foaf:firstName ?fn . }}
  OPTIONAL {{ ?senatore foaf:lastName  ?ln . }}
  BIND(REPLACE(CONCAT(COALESCE(?ln,''), ' ', COALESCE(?fn,'')), '^\\s+|\\s+$', '') AS ?nomeCompleto)

  {{
    ?senatore ocd:aderisce ?adesione .
    ?adesione osr:gruppo ?gruppo .
    OPTIONAL {{ ?adesione dc:date ?adesioneData . }}
    OPTIONAL {{ ?adesione osr:inizio ?inizio . }}
    OPTIONAL {{ ?adesione osr:fine ?fine . }}
  }}
  UNION
  {{
    ?senatore osr:mandato ?mandato .
    ?mandato ocd:aderisce ?adesione .
    ?adesione osr:gruppo ?gruppo .
    OPTIONAL {{ ?adesione dc:date ?adesioneData . }}
    OPTIONAL {{ ?adesione osr:inizio ?inizio . }}
    OPTIONAL {{ ?adesione osr:fine ?fine . }}
  }}
  UNION
  {{
    ?senatore osr:mandato ?mandato .
    ?mandato ocd:rif_gruppoParlamentare ?gruppo .
    OPTIONAL {{ ?mandato dc:date ?adesioneData . }}
    OPTIONAL {{ ?mandato osr:inizio ?inizio . }}
    OPTIONAL {{ ?mandato osr:fine ?fine . }}
  }}

  OPTIONAL {{
    ?gruppo osr:denominazione ?denom .
    ?denom osr:titolo ?gruppoTitolo .
  }}
  OPTIONAL {{ ?gruppo rdfs:label ?gruppoLabel . }}
  BIND(COALESCE(?gruppoTitolo, ?gruppoLabel) AS ?gruppoTitolo)

  FILTER(!BOUND(?inizio) || STR(?inizio) <= "{act_date_iso}")
  FILTER(!BOUND(?fine)   || STR(?fine)   >= "{act_date_iso}")
}}
ORDER BY ?nomeCompleto ?gruppoTitolo
""".strip()


def fetch_groups_for_act(act_uri: str, act_date_iso: str) -> str:
    if not act_uri or not act_date_iso:
        return "-"
    try:
        rows = run_sparql(_act_groups_query(act_uri, act_date_iso))
    except Exception as exc:
        LOG.warning("Lookup gruppi fallito per %s: %s", act_uri, exc)
        return "-"

    groups = [_value(row, "gruppoTitolo") for row in rows]
    return _join(groups, sep=" | ", fallback="-")


def fetch_recent_sindisp(days_back: int = 7, legislatura: Optional[str] = None) -> List[Dict]:
    date_from = _iso_date_days_ago(days_back)
    leg_filter = f'FILTER(STR(?legislatura) = "{legislatura}")' if legislatura else ""

    query = f"""
SELECT ?atto ?legislatura ?numero ?tipo ?data ?url ?esito
       (GROUP_CONCAT(DISTINCT ?firmatario; separator=" | ") AS ?firmatariSparql)
WHERE {{
  ?atto a osr:SindacatoIspettivo ;
        osr:legislatura ?legislatura ;
        osr:dataPresentazione ?data ;
        osr:numero ?numero .
  OPTIONAL {{ ?atto osr:tipo ?tipo . }}
  OPTIONAL {{ ?atto osr:URLTesto ?url . }}
  OPTIONAL {{ ?atto osr:esito ?esito . }}
  OPTIONAL {{
    ?atto osr:iniziativa ?iniziativa .
    ?iniziativa osr:senatore ?senatore .
    OPTIONAL {{ ?senatore foaf:firstName ?fn . }}
    OPTIONAL {{ ?senatore foaf:lastName ?ln . }}
    BIND(REPLACE(CONCAT(COALESCE(?ln,''), ' ', COALESCE(?fn,'')), '^\\s+|\\s+$', '') AS ?firmatario)
  }}
  FILTER(STR(?data) >= "{date_from}")
  {leg_filter}
}}
GROUP BY ?atto ?legislatura ?numero ?tipo ?data ?url ?esito
ORDER BY DESC(?data) DESC(?numero)
""".strip()

    rows = run_sparql(query)

    items = []
    for row in rows:
        act_date = _parse_iso_date(_value(row, "data"))
        if act_date and act_date < dt.date.fromisoformat(date_from):
            continue

        raw_url = _value(row, "url")
        public_url = resolve_public_url(raw_url, tipodoc_hint="Sindisp") if raw_url else raw_url
        enrich = enrich_sindisp_from_page(public_url) if public_url else {}

        act_uri = _value(row, "atto")
        act_date_iso = act_date.isoformat() if act_date else ""
        groups = fetch_groups_for_act(act_uri, act_date_iso)

        items.append(
            {
                "fonte": "Senato",
                "categoria_atto": "Sindisp",
                "uri": act_uri,
                "legislatura": _value(row, "legislatura"),
                "tipo": _value(row, "tipo", "Sindacato ispettivo"),
                "numero": _value(row, "numero"),
                "data_presentazione": act_date_iso,
                "stato": enrich.get("status") or _value(row, "esito") or "-",
                "a_chi": enrich.get("target", "-"),
                "proponenti": enrich.get("proponents") or _value(row, "firmatariSparql") or "-",
                "gruppo": groups,
                "link": enrich.get("public_link") or public_url or raw_url or "-",
                "testo": enrich.get("page_text", ""),
            }
        )
    return items


def fetch_recent_ddl(days_back: int = 7, legislatura: Optional[str] = None) -> List[Dict]:
    date_from = _iso_date_days_ago(days_back)
    leg_filter = f'FILTER(STR(?legislatura) = "{legislatura}")' if legislatura else ""

    query = f"""
SELECT DISTINCT ?ddl ?legislatura ?numero ?data ?titolo ?stato ?dataStato ?url ?natura WHERE {{
  ?ddl a osr:Ddl ;
       osr:legislatura ?legislatura ;
       osr:dataPresentazione ?data .
  OPTIONAL {{ ?ddl osr:numero ?numero . }}
  OPTIONAL {{ ?ddl osr:titolo ?titolo . }}
  OPTIONAL {{ ?ddl osr:statoDdl ?stato . }}
  OPTIONAL {{ ?ddl osr:dataStatoDdl ?dataStato . }}
  OPTIONAL {{ ?ddl osr:URLTesto ?url . }}
  OPTIONAL {{ ?ddl osr:natura ?natura . }}
  FILTER(STR(?data) >= "{date_from}")
  {leg_filter}
}}
ORDER BY DESC(?data) DESC(?numero)
""".strip()

    rows = run_sparql(query)
    items = []
    for row in rows:
        act_date = _parse_iso_date(_value(row, "data"))
        if act_date and act_date < dt.date.fromisoformat(date_from):
            continue
        items.append(
            {
                "fonte": "Senato",
                "categoria_atto": "DDL",
                "uri": _value(row, "ddl"),
                "legislatura": _value(row, "legislatura"),
                "tipo": "Disegno di legge",
                "numero": _value(row, "numero"),
                "data_presentazione": act_date.isoformat() if act_date else "",
                "stato": _value(row, "stato") or "-",
                "a_chi": "-",
                "proponenti": "-",
                "gruppo": "-",
                "titolo": _value(row, "titolo"),
                "link": _value(row, "url") or "-",
                "testo": _value(row, "titolo"),
            }
        )
    return items
