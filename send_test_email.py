import os
import smtplib
import time
import urllib.request
import urllib.error
from email.message import EmailMessage
from datetime import datetime
from zoneinfo import ZoneInfo

from senato_sparql import fetch_senato_last_48h


SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

SOURCES_WARNINGS: list[str] = []


def check_url(url: str, timeout_s: int = 15, retries: int = 3, backoff_s: int = 5) -> bool:
    """
    Simple reachability check with retries/backoff.
    If it fails, it appends a warning to SOURCES_WARNINGS and returns False.
    """
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


def render_section(title: str, items: list, empty_line: str) -> str:
    lines = [f"{title}", "-" * len(title)]
    if not items:
        lines.append(empty_line)
        return "\n".join(lines)

    for it in items:
        line = f"- {it.get('branch','?')} | {it.get('act_id', it.get('act_ref','?'))} | {it.get('title','(senza titolo)')}"
        # se presente, mostra data presentazione (non è la deadline)
        if it.get("date"):
            line += f" | data: {it['date']}"
        if it.get("deadline_dt"):
            line += f" | scadenza: {it['deadline_dt']}"
        if it.get("why"):
            line += f" | perché: {it['why']}"
        if it.get("url"):
            line += f" | {it['url']}"
        lines.append(line)
        if it.get("evidence"):
            lines.append(f"  evidenza: {it['evidence']}")
    return "\n".join(lines)


def main() -> None:
    username = os.environ["SMTP_USERNAME"]
    password = os.environ["SMTP_PASSWORD"]
    to_email = os.environ["ALERT_TO_EMAIL"]

    now_rome = datetime.now(ZoneInfo("Europe/Rome")).strftime("%Y-%m-%d %H:%M")

      # --- Step corrente: check + prima estrazione reale Senato (DDL + Sindacato Ispettivo, ultime 48h) ---
    senato_up = check_url("https://dati.senato.it/sparql")

    senato_ddls = []
    senato_sind = []
    if senato_up:
        try:
            ddls, sind, warn = fetch_senato_last_48h(limit_each=10, days=2)
            senato_ddls = ddls
            senato_sind = sind
            SOURCES_WARNINGS.extend(warn)
        except Exception as e:
            SOURCES_WARNINGS.append(
                f"Errore imprevisto durante fetch_senato_last_48h: {type(e).__name__}: {e}"
            )

    # --- Placeholder: qui in futuro metteremo i risultati "marittimi" veri ---
    relevant_items = []   # list of dict: {branch, act_id, title, url, why}
    deadlines = []        # list of dict: {branch, act_ref, deadline_dt, url, evidence}
    borderline_items = [] # list of dict

    body = "\n\n".join([
        "Monitor Parlamento — Trasporto marittimo",
        f"Generato: {now_rome} (Europe/Rome)",
        "",
        "Sorgenti / Warning",
        "------------------",
        "\n".join([f"- {w}" for w in SOURCES_WARNINGS]) if SOURCES_WARNINGS else "- Nessun problema rilevato sulle sorgenti.",
        "",
        "Senato — Ultime 48h (DDL + Sindacato Ispettivo)",
        "---------------------------------------------",
        render_section(
            "DDL (ultimi 10)",
            senato_ddls,
            "Nessun DDL trovato nelle ultime 48h (o sorgente non disponibile)."
        ),
        "",
        render_section(
            "Sindacato Ispettivo (ultimi 10, P1)",
            senato_sind,
            "Nessun atto di sindacato ispettivo trovato nelle ultime 48h (o sorgente non disponibile)."
        ),
        "",
        render_section(
            "1) Atti rilevanti (marittimo)",
            relevant_items,
            "Nessun atto rilevante trovato (placeholder)."
        ),
        render_section(
            "2) Scadenze emendamenti (nuove o cambiate)",
            deadlines,
            "Nessuna scadenza trovata (placeholder)."
        ),
        render_section(
            "3) Borderline (da rivedere)",
            borderline_items,
            "Nessun caso borderline (placeholder)."
        ),
    ])

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


if __name__ == "__main__":
    main()
