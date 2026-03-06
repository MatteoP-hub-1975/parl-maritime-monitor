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

def _http_get(url: str, timeout_s: int = 20) -> str:
    """
    GET con header 'browser-like' per ridurre blocchi/403.
    urllib segue i redirect automaticamente.
    """
    url = _normalize_url(url)
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
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", " ", html)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def _looks_like_error_page(html: str) -> bool:
    """
    Check 'soft' per capire se la pagina è un errore Senato.
    (Molti errori Senato includono /Errore_403/ o testi tipo "Errore 403".)
    """
    if not html:
        return True
    h = html.lower()
    bad_markers = [
        "/errore_403/",
        "errore 403",
        "accesso negato",
        "errore",
        "pagina non trovata",
        "404",
        "non disponibile",
        "forbidden",
        "matomo.cloud",   # spesso nelle pagine di errore Senato
    ]
    # se contiene "atto senato" o "atto camera" è quasi certamente ok
    if "atto senato" in h or "atto camera" in h:
        return False
    # se contiene molti marker di errore, scartiamo
    score = sum(1 for m in bad_markers if m in h)
    return score >= 2


def _url_seems_ok(url: str, timeout_s: int = 15) -> bool:
    try:
        html = _http_get(url, timeout_s=timeout_s)
        return not _looks_like_error_page(html)
    except Exception:
        return False


def _normalize_url(url: str) -> str:
    if not url:
        return ""
    u = url.strip().replace("\xa0", " ")
    # forza https su senato.it / dati.senato.it
    if u.startswith("http://www.senato.it/"):
        u = "https://www.senato.it/" + u[len("http://www.senato.it/"):]
    if u.startswith("http://dati.senato.it/"):
        u = "https://dati.senato.it/" + u[len("http://dati.senato.it/"):]
    return u


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

    # 1) GET (preferito)
    params = urllib.parse.urlencode({"query": query, "format": "application/sparql-results+json"})
    url = f"{SENATO_SPARQL_ENDPOINT}?{params}"

    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            payload = resp.read().decode("utf-8", errors="replace")
            return json.loads(payload)
    except urllib.error.HTTPError as he:
        # fallback POST solo su certi errori
        if he.code not in (400, 403, 429):
            raise

    # 2) POST fallback
    data = urllib.parse.urlencode({"query": query}).encode("utf-8")
    headers_post = dict(headers)
    headers_post["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"

    try:
        req = urllib.request.Request(SENATO_SPARQL_ENDPOINT, data=data, headers=headers_post, method="POST")
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


def _request_with_retries(query: str, timeout_s: int = 25, retries: int = 3, backoff_s: int = 4) -> Dict[str, Any]:
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
# Sindacato link builder
# -------------------------

def _extract_doc_id(uri_or_url: str) -> str:
    if not uri_or_url:
        return ""
    u = uri_or_url.strip()
    try:
        parsed = urllib.parse.urlparse(u)
        qs = urllib.parse.parse_qs(parsed.query)
        if "id" in qs and qs["id"]:
            return (qs["id"][0] or "").strip()
    except Exception:
        pass
    m = re.search(r"(\d{6,})", u)
    return m.group(1) if m else ""


def _sindisp_candidates(doc_id: str, leg: str) -> List[str]:
    doc_id = (doc_id or "").strip()
    leg = (leg or "19").strip() or "19"
    if not doc_id:
        return []
    # NB: due varianti show-doc: alcune pagine rispondono meglio a una o all'altra
    return [
        f"{SENATO_BASE}/show-doc?id={doc_id}&leg={leg}&tipodoc=Sindisp",
        f"{SENATO_BASE}/show-doc?id={doc_id}&idoggetto=0&leg={leg}&tipodoc=Sindisp",
        f"{SENATO_BASE}/show-doc?tipodoc=Sindisp&leg={leg}&id={doc_id}",
        f"{SENATO_BASE}/loc/link.asp?tipodoc=sindisp&leg={leg}&id={doc_id}",
    ]


def _choose_best_url(candidates: List[str]) -> str:
    seen = set()
    cand = []
    for u in candidates:
        u = _normalize_url(u)
        if not u or u in seen:
            continue
        seen.add(u)
        cand.append(u)

    # prova veloce: il primo che "sembra ok"
    for u in cand:
        if _url_seems_ok(u, timeout_s=12):
            return u

    # fallback: primo in lista
    return cand[0] if cand else ""


# -------------------------
# Optional: enrich sindisp metadata from HTML
# (titolo se presente, destinatario, proponente, gruppo)
# -------------------------

def _enrich_sindisp_from_page(url: str) -> Dict[str, str]:
    """
    Estrazione 'best effort'. Se non troviamo, lasciamo stringhe vuote.
    """
    out = {
        "title": "",
        "destinatario": "",
        "proponenti": "",
        "gruppo": "",
    }
    try:
        html = _http_get(url, timeout_s=20)
        if _looks_like_error_page(html):
            return out

        text = _strip_html_to_text(html)

        # proponente (prima firma) - spesso "presentata daNOME COGNOME"
        m = re.search(r"presentata\s+da\s*([A-ZÀ-Ü'’\- ]{3,80})", text, re.I)
        if m:
            out["proponenti"] = m.group(1).strip()

        # destinatario - spesso tra trattini: "-Al Ministro ...-" / "-Ai Ministri ...-"
        md = re.search(r"-\s*A(i|l)\s+Ministr[oi][^-]{5,250}-", text, re.I)
        if md:
            out["destinatario"] = md.group(0).strip().strip("-").strip()

        # gruppo - a volte compare come "Gruppo: XXX"
        mg = re.search(r"\bGruppo\s*:\s*([^\n]{2,80})", text, re.I)
        if mg:
            out["gruppo"] = mg.group(1).strip()

        # titolo "se presente": alcune pagine hanno "Oggetto:" o simili
        mt = re.search(r"\bOggetto\s*:\s*([^\n]{5,180})", text, re.I)
        if mt:
            out["title"] = mt.group(1).strip()

        return out
    except Exception:
        return out


# -------------------------
# DDL enrichment: commissione (best effort)
# -------------------------

def _enrich_ddl_commissione(url: str) -> str:
    try:
        html = _http_get(url, timeout_s=25)
        text = _strip_html_to_text(html)

        m = re.search(r"Assegnat[oa]\s+alla\s+([^.\n]+)", text, re.I)
        if m:
            return m.group(1).strip()

        m2 = re.search(r"\b(\d{1,2}[ªa]\s+Commissione[^.\n]+)", text)
        if m2:
            return m2.group(1).strip()

        return ""
    except Exception:
        return ""


# -------------------------
# Public API
# -------------------------

def fetch_senato_last_48h(
    limit_each: int = 200,
    days: int = 2,
    enrich_sindisp: bool = False,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[str]]:
    """
    Returns (ddls, sindacato_ispettivo, warnings)
    """
    warnings: List[str] = []

    now = datetime.now(TZ_ROME) if TZ_ROME else datetime.now()
    start_date = (now - timedelta(days=days)).date().isoformat()  # YYYY-MM-DD

    # ---- DDL ----
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

    # ---- Sindacato Ispettivo (query MINIMA + dedup + fix link) ----
    q_sind = f"""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?s ?tipo ?numero ?data ?url ?leg ?esito
WHERE {{
  ?s rdf:type <http://dati.senato.it/osr/SindacatoIspettivo> .
  OPTIONAL {{ ?s <http://dati.senato.it/osr/tipo> ?tipo . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/numero> ?numero . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/dataPresentazione> ?data . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/URLTesto> ?url . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/legislatura> ?leg . }}
  OPTIONAL {{ ?s <http://dati.senato.it/osr/esito> ?esito . }}

  FILTER(BOUND(?data))
  FILTER(STR(?data) >= "{start_date}")
  FILTER(BOUND(?tipo))
  FILTER(
    CONTAINS(LCASE(STR(?tipo)), "interrogazione") ||
    CONTAINS(LCASE(STR(?tipo)), "interpellanza") ||
    CONTAINS(LCASE(STR(?tipo)), "mozione") ||
    CONTAINS(LCASE(STR(?tipo)), "risoluzione")
  )
}}
ORDER BY DESC(?data)
LIMIT 800
""".strip()

    ddls: List[Dict[str, str]] = []
    sind: List[Dict[str, str]] = []

    # --- DDL fetch ---
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

            url = (
                f"{SENATO_BASE}/leggi-e-documenti/disegni-di-legge/scheda-ddl?did={id_fase}"
                if id_fase
                else _normalize_url((r.get("ddl", "") or "").strip())
            )

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

    # --- Sindacato fetch (dedup) ---
    try:
        res = _request_with_retries(q_sind)
        rows = _bindings_to_rows(res.get("results", {}).get("bindings", []))

        # raggruppa per numero, perché i duplicati spesso differiscono solo per URL/URI
        grouped: Dict[str, List[Dict[str, str]]] = {}
        for r in rows:
            numero = (r.get("numero", "") or "").strip()
            if not numero:
                continue
            grouped.setdefault(numero, []).append(r)

        for numero, variants in grouped.items():
            # per ogni numero, costruiamo candidati e scegliamo il migliore
            best_url = ""
            best_payload: Optional[Dict[str, str]] = None

            for r in variants:
                act_uri = _normalize_url((r.get("s", "") or "").strip())
                tipo = (r.get("tipo", "") or "").strip() or "Sindacato ispettivo"
                data_pres = (r.get("data", "") or "").strip()
                esito = (r.get("esito", "") or "").strip()
                leg = (r.get("leg", "") or "19").strip() or "19"
                urltesto = _normalize_url((r.get("url", "") or "").strip())

                doc_id = _extract_doc_id(urltesto) or _extract_doc_id(act_uri)
                candidates = []
                if doc_id:
                    candidates.extend(_sindisp_candidates(doc_id, leg))
                # aggiungi anche eventuali url in chiaro
                if urltesto:
                    candidates.append(urltesto)
                if act_uri:
                    candidates.append(act_uri)

                chosen = _choose_best_url(candidates) if candidates else (urltesto or act_uri)

                # Se abbiamo già un best, preferiamo quello che sembra ok
                if not best_url:
                    best_url = chosen
                    best_payload = {
                        "tipo": tipo,
                        "numero": numero,
                        "date_presentazione": data_pres,
                        "stato": esito,
                        "leg": leg,
                    }
                else:
                    # se il nuovo sembra ok e l'attuale no, sostituisci
                    if _url_seems_ok(chosen, timeout_s=10) and not _url_seems_ok(best_url, timeout_s=10):
                        best_url = chosen
                        best_payload = {
                            "tipo": tipo,
                            "numero": numero,
                            "date_presentazione": data_pres,
                            "stato": esito,
                            "leg": leg,
                        }

            if not best_payload:
                continue

            row_out = {
                "branch": "Senato",
                "tipo": best_payload["tipo"],
                "numero": best_payload["numero"],
                "title": "",
                "url": best_url,
                "date_presentazione": best_payload["date_presentazione"],
                "destinatario": "",
                "proponenti": "",
                "gruppo": "",
                "stato": best_payload["stato"],
            }

            if enrich_sindisp and best_url:
                extra = _enrich_sindisp_from_page(best_url)
                row_out.update(extra)

            sind.append(row_out)

        # ordina per data decrescente (stringa YYYY-MM-DD funziona)
        sind.sort(key=lambda x: (x.get("date_presentazione", ""), x.get("numero", "")), reverse=True)

    except Exception as e:
        warnings.append(f"Query Sindacato Ispettivo fallita su SPARQL Senato: {type(e).__name__}: {e}")

    return ddls, sind, warnings
