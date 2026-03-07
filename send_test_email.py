import os
import re
import smtplib
import time
import unicodedata
import urllib.request
from email.message import EmailMessage
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

import yaml

from senato_sparql import fetch_senato_last_48h

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

SOURCES_WARNINGS: list[str] = []


def check_url(url: str, timeout_s: int = 15, retries: int = 3, backoff_s: int = 5) -> bool:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                url,
                method="GET",
                headers={"User-Agent": "parl-maritime-monitor/0.1 (GitHub Actions)"},
            )
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                return 200 <= resp.status < 400
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_s * attempt)
            else:
                SOURCES_WARNINGS.append(
                    f"Senato SPARQL non disponibile: {url} (ultimo errore: {type(last_err).__name__}: {last_err})"
                )
                return False
    return False


# =========================
# KB + NORMALIZZAZIONE
# =========================

def load_kb(path: str = "kb.yaml") -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def flatten_kb_section(section: Any) -> set[str]:
    values: set[str] = set()

    def _walk(obj: Any) -> None:
        if isinstance(obj, str):
            val = normalize_text(obj)
            if val:
                values.add(val)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)
        elif isinstance(obj, dict):
            for v in obj.values():
                _walk(v)

    _walk(section)
    return values


def build_kb_index(kb: dict[str, Any]) -> dict[str, set[str]]:
    return {
        "keywords": flatten_kb_section(kb.get("keywords", {})),
        "keyphrases": flatten_kb_section(kb.get("keyphrases", {})),
        "norm_refs": flatten_kb_section(kb.get("norm_refs", {})),
        "entities": flatten_kb_section(kb.get("entities", {})),
    }


# =========================
# CLASSIFICAZIONE
# =========================

def find_hits(text_norm: str, terms: set[str]) -> list[str]:
    if not text_norm:
        return []

    padded = f" {text_norm} "
    hits: list[str] = []

    for term in terms:
        if not term:
            continue
        needle = f" {term} "
        if needle in padded:
            hits.append(term)

    return sorted(set(hits))


def is_obviously_non_sector(title_norm: str) -> bool:
    obvious_non_sector_terms = [
        "musica",
        "conservatorio",
        "bicicletta",
        "biciclette",
        "cinema",
        "beni archeologici",
        "sport dilettantistico",
        "spettacolo dal vivo",
        "universita telematica",
    ]
    padded = f" {title_norm} "
    return any(f" {term} " in padded for term in obvious_non_sector_terms)


def is_borderline_omnibus(title_norm: str) -> bool:
    omnibus_terms = [
        "bilancio",
        "legge europea",
        "decreto legge",
        "milleproroghe",
        "semplificazioni",
        "infrastrutture",
        "concorrenza",
        "misure urgenti",
        "disposizioni urgenti",
        "delega al governo",
    ]
    padded = f" {title_norm} "
    return any(f" {term} " in padded for term in omnibus_terms)


def score_hits(title_hits: dict[str, list[str]], text_hits: dict[str, list[str]]) -> int:
    score = 0

    score += len(title_hits["keywords"]) * 2
    score += len(title_hits["keyphrases"]) * 4
    score += len(title_hits["norm_refs"]) * 4
    score += len(title_hits["entities"]) * 3

    score += len(text_hits["keywords"]) * 1
    score += len(text_hits["keyphrases"]) * 2
    score += len(text_hits["norm_refs"]) * 3
    score += len(text_hits["entities"]) * 2

    return score


def classify_act(title: str, body_text: str, kb_index: dict[str, set[str]]) -> dict[str, Any]:
    title_norm = normalize_text(title)
    body_norm = normalize_text(body_text)

    title_hits = {
        "keywords": find_hits(title_norm, kb_index["keywords"]),
        "keyphrases": find_hits(title_norm, kb_index["keyphrases"]),
        "norm_refs": find_hits(title_norm, kb_index["norm_refs"]),
        "entities": find_hits(title_norm, kb_index["entities"]),
    }

    text_hits = {
        "keywords": find_hits(body_norm, kb_index["keywords"]),
        "keyphrases": find_hits(body_norm, kb_index["keyphrases"]),
        "norm_refs": find_hits(body_norm, kb_index["norm_refs"]),
        "entities": find_hits(body_norm, kb_index["entities"]),
    }

    title_score = sum(len(v) for v in title_hits.values())
    text_score = sum(len(v) for v in text_hits.values())
    total_score = score_hits(title_hits, text_hits)

    if is_obviously_non_sector(title_norm) and title_score == 0:
        return {
            "sector_relevant": False,
            "reason": "Titolo chiaramente estraneo al settore",
            "score": total_score,
            "title_hits": title_hits,
            "text_hits": text_hits,
        }

    if title_score > 0:
        return {
            "sector_relevant": True,
            "reason": "Match KB sul titolo",
            "score": total_score,
            "title_hits": title_hits,
            "text_hits": text_hits,
        }

    if is_borderline_omnibus(title_norm):
        if text_score > 0:
            return {
                "sector_relevant": True,
                "reason": "Titolo borderline ma match KB sul testo",
                "score": total_score,
                "title_hits": title_hits,
                "text_hits": text_hits,
            }
        return {
            "sector_relevant": False,
            "reason": "Titolo borderline senza match KB nel testo",
            "score": total_score,
            "title_hits": title_hits,
            "text_hits": text_hits,
        }

    if text_score > 0:
        return {
            "sector_relevant": True,
            "reason": "Match KB sul testo",
            "score": total_score,
            "title_hits": title_hits,
            "text_hits": text_hits,
        }

    return {
        "sector_relevant": False,
        "reason": "Nessun match KB",
        "score": total_score,
        "title_hits": title_hits,
        "text_hits": text_hits,
    }


# =========================
# NORMALIZZAZIONE RECORD
# =========================

def safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def build_unified_acts(senato_ddls: list[dict], senato_sind: list[dict]) -> list[dict]:
    acts: list[dict] = []

    for it in senato_ddls:
        acts.append({
            "kind": "ddl",
            "branch": safe_str(it.get("branch") or "Senato"),
            "numero": safe_str(it.get("ddl_number")),
            "titolo": safe_str(it.get("title") or it.get("titolo")),
            "data_presentazione": safe_str(it.get("date_presentazione")),
            "iniziativa": safe_str(it.get("iniziativa")),
            "stato": safe_str(it.get("stato")),
            "commissione": safe_str(it.get("commissione")),
            "link": safe_str(it.get("url")),
            "testo": safe_str(it.get("testo") or it.get("body_text") or ""),
        })

    for it in senato_sind:
        acts.append({
            "kind": "sindisp",
            "branch": safe_str(it.get("branch") or "Senato"),
            "tipo": safe_str(it.get("tipo")),
            "titolo": safe_str(it.get("titolo") or it.get("title")),
            "destinatari": safe_str(it.get("destinatario") or it.get("destinatari")),
            "numero": safe_str(it.get("numero")),
            "proponenti": safe_str(it.get("proponente") or it.get("proponenti")),
            "gruppo": safe_str(it.get("gruppo")),
            "stato": safe_str(it.get("stato")),
            "link": safe_str(it.get("url")),
            "testo": safe_str(it.get("testo") or it.get("body_text") or ""),
        })

    return acts


# =========================
# RENDER EMAIL
# =========================

def format_ddl_item(act: dict[str, Any]) -> str:
    lines = [
        "Senato",
        f"Numero DDL: {act.get('numero') or '-'}",
        f"Titolo: {act.get('titolo') or '-'}",
        f"Data presentazione: {act.get('data_presentazione') or '-'}",
        f"Iniziativa: {act.get('iniziativa') or '-'}",
        f"Stato: {act.get('stato') or '-'}",
        f"Commissione: {act.get('commissione') or '-'}",
        f"Link: {act.get('link') or '-'}",
    ]
    return "\n".join(lines)


def format_sindisp_item(act: dict[str, Any]) -> str:
    lines = [
        "Senato",
        f"Tipo: {act.get('tipo') or '-'}",
        f"Titolo: {act.get('titolo') or '-'}",
        f"A chi è rivolta: {act.get('destinatari') or '-'}",
        f"Numero: {act.get('numero') or '-'}",
        f"Proponente/i: {act.get('proponenti') or '-'}",
        f"Gruppo parlamentare: {act.get('gruppo') or '-'}",
        f"Stato: {act.get('stato') or '-'}",
        f"Link: {act.get('link') or '-'}",
    ]
    return "\n".join(lines)


def format_act_for_email(act: dict[str, Any]) -> str:
    if act.get("kind") == "ddl":
        return format_ddl_item(act)
    if act.get("kind") == "sindisp":
        return format_sindisp_item(act)

    lines = [
        f"Tipo atto: {act.get('kind') or '-'}",
        f"Titolo: {act.get('titolo') or '-'}",
        f"Link: {act.get('link') or '-'}",
    ]
    return "\n".join(lines)


def render_section(title: str, acts: list[dict[str, Any]], empty_text: str) -> str:
    lines = [title, "-" * len(title)]

    if not acts:
        lines.append(empty_text)
        return "\n".join(lines)

    for idx, act in enumerate(acts, start=1):
        lines.append(f"[{idx}]")
        lines.append(format_act_for_email(act))
        lines.append("")

    return "\n".join(lines).rstrip()


def build_email_body(
    now_rome: str,
    relevant_acts: list[dict[str, Any]],
    non_relevant_acts: list[dict[str, Any]],
) -> str:
    warnings_block = (
        "\n".join([f"- {w}" for w in SOURCES_WARNINGS])
        if SOURCES_WARNINGS
        else "- Nessun problema rilevato sulle sorgenti."
    )

    parts = [
        "Monitor Parlamento — Trasporto marittimo",
        f"Generato: {now_rome} (Europe/Rome)",
        "",
        "Sorgenti / Warning",
        "------------------",
        warnings_block,
        "",
        render_section(
            "Riguarda il settore",
            relevant_acts,
            "Nessun atto rilevante trovato.",
        ),
        "",
        render_section(
            "Non riguarda il settore",
            non_relevant_acts,
            "Nessun atto non rilevante trovato.",
        ),
    ]

    return "\n".join(parts)


def main() -> None:
    username = os.environ["SMTP_USERNAME"]
    password = os.environ["SMTP_PASSWORD"]
    to_email = os.environ["ALERT_TO_EMAIL"]

    now_rome = datetime.now(ZoneInfo("Europe/Rome")).strftime("%Y-%m-%d %H:%M")

    try:
        kb = load_kb("kb.yaml")
        kb_index = build_kb_index(kb)
    except Exception as e:
        SOURCES_WARNINGS.append(
            f"Errore caricamento kb.yaml: {type(e).__name__}: {e}"
        )
        kb_index = {
            "keywords": set(),
            "keyphrases": set(),
            "norm_refs": set(),
            "entities": set(),
        }

    senato_up = check_url("https://dati.senato.it/sparql")

    senato_ddls: list[dict] = []
    senato_sind: list[dict] = []

    if senato_up:
        try:
            ddls, sind, warn = fetch_senato_last_48h(limit_each=200, days=2)
            senato_ddls = ddls
            senato_sind = sind
            SOURCES_WARNINGS.extend(warn)
        except Exception as e:
            SOURCES_WARNINGS.append(
                f"Errore imprevisto durante fetch_senato_last_48h: {type(e).__name__}: {e}"
            )

    acts = build_unified_acts(senato_ddls, senato_sind)

    relevant_acts: list[dict[str, Any]] = []
    non_relevant_acts: list[dict[str, Any]] = []

    for act in acts:
        classification = classify_act(
            title=act.get("titolo", ""),
            body_text=act.get("testo", ""),
            kb_index=kb_index,
        )
        act["classification"] = classification

        if classification["sector_relevant"]:
            relevant_acts.append(act)
        else:
            non_relevant_acts.append(act)

    body = build_email_body(
        now_rome=now_rome,
        relevant_acts=relevant_acts,
        non_relevant_acts=non_relevant_acts,
    )

    msg = EmailMessage()
    msg["Subject"] = f"Monitor Parlamento (marittimo) — {now_rome}"
    msg["From"] = username
    msg["To"] = to_email
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.ehlo()
        s.starttls()
        s.login(username, password)
        s.send_message(msg)

    print("Email inviata a", to_email)
    print("Atti rilevanti:", len(relevant_acts))
    print("Atti non rilevanti:", len(non_relevant_acts))


if __name__ == "__main__":
    main()
