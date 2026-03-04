import os
import smtplib
import time
import urllib.request
from email.message import EmailMessage
from datetime import datetime
from zoneinfo import ZoneInfo

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


def render_senato_ddls(items: list[dict]) -> str:
    title = "DDL Senato (ultimi 10)"
    lines = [title, "-" * len(title)]
    if not items:
        lines.append("Nessun DDL trovato nelle ultime 48h (o sorgente non disponibile).")
        return "\n".join(lines)

    for it in items:
        # Campi richiesti: ramo, n.ddl, titolo, data presentazione, iniziativa, stato, commissione, link
        ramo = it.get("branch", "Senato")
        n = (it.get("ddl_number") or "").strip()
        ddl = f"DDL {n}".strip() if n else "DDL"
        titolo = it.get("title", "(senza titolo)").strip()

        chunks = [f"- {ramo} | {ddl} | {titolo}"]

        dp = (it.get("date_presentazione") or "").strip()
        if dp:
            chunks.append(f"data presentazione: {dp}")

        iniziativa = (it.get("iniziativa") or "").strip()
        if iniziativa:
            chunks.append(f"iniziativa: {iniziativa}")

        stato = (it.get("stato") or "").strip()
        if stato:
            chunks.append(f"stato: {stato}")

        comm = (it.get("commissione") or "").strip()
        if comm:
            chunks.append(f"commissione: {comm}")

        url = (it.get("url") or "").strip()
        if url:
            chunks.append(url)

        lines.append(" | ".join(chunks))

    return "\n".join(lines)


def render_senato_sindisp(items: list[dict]) -> str:
    title = "Sindacato ispettivo Senato (ultimi 10, P1)"
    lines = [title, "-" * len(title)]
    if not items:
        lines.append("Nessun atto di sindacato ispettivo trovato nelle ultime 48h (o sorgente non disponibile).")
        return "\n".join(lines)

    for it in items:
        ramo = it.get("branch", "Senato")
        tipo = (it.get("tipo") or "").strip() or "Sindacato ispettivo"
        numero = (it.get("numero") or "").strip()

        chunks = [f"- {ramo} | {tipo}"]

        # titolo (se presente)
        tit = (it.get("titolo") or "").strip()
        if tit:
            chunks.append(tit)

        # a chi è rivolta (se presente)
        dest = (it.get("destinatario") or "").strip()
        if dest:
            chunks.append(f"a: {dest}")

        # numero (sempre, se disponibile)
        if numero:
            chunks.append(f"n.: {numero}")

        # proponente
        prop = (it.get("proponente") or "").strip()
        if prop:
            chunks.append(f"proponente: {prop}")

        # gruppo
        gruppo = (it.get("gruppo") or "").strip()
        if gruppo:
            chunks.append(f"gruppo: {gruppo}")

        # stato
        stato = (it.get("stato") or "").strip()
        if stato:
            chunks.append(f"stato: {stato}")

        # link show-doc
        url = (it.get("url") or "").strip()
        if url:
            chunks.append(url)

        lines.append(" | ".join(chunks))

    return "\n".join(lines)


def render_simple_section(title: str, items: list, empty_line: str) -> str:
    lines = [title, "-" * len(title)]
    if not items:
        lines.append(empty_line)
        return "\n".join(lines)
    for it in items:
        lines.append(f"- {it}")
    return "\n".join(lines)


def main() -> None:
    username = os.environ["SMTP_USERNAME"]
    password = os.environ["SMTP_PASSWORD"]
    to_email = os.environ["ALERT_TO_EMAIL"]

    now_rome = datetime.now(ZoneInfo("Europe/Rome")).strftime("%Y-%m-%d %H:%M")

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

    # Placeholder (marittimo + scadenze) — li riempiamo dopo
    relevant_items = []
    deadlines = []
    borderline_items = []

    body = "\n\n".join([
        "Monitor Parlamento — Trasporto marittimo",
        f"Generato: {now_rome} (Europe/Rome)",
        "",
        "Sorgenti / Warning",
        "------------------",
        "\n".join([f"- {w}" for w in SOURCES_WARNINGS]) if SOURCES_WARNINGS else "- Nessun problema rilevato sulle sorgenti.",
        "",
        "Senato — Ultime 48h",
        "------------------",
        render_senato_ddls(senato_ddls),
        "",
        render_senato_sindisp(senato_sind),
        "",
        "1) Atti rilevanti (marittimo)",
        "-----------------------------",
        "Nessun atto rilevante trovato (placeholder).",
        "",
        "2) Scadenze emendamenti (nuove o cambiate)",
        "------------------------------------------",
        "Nessuna scadenza trovata (placeholder).",
        "",
        "3) Borderline (da rivedere)",
        "---------------------------",
        "Nessun caso borderline (placeholder).",
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
