import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
from zoneinfo import ZoneInfo

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

username = os.environ["SMTP_USERNAME"]
password = os.environ["SMTP_PASSWORD"]
to_email = os.environ["ALERT_TO_EMAIL"]

now_rome = datetime.now(ZoneInfo("Europe/Rome")).strftime("%Y-%m-%d %H:%M")

# --- Placeholder: qui in futuro metteremo i risultati veri ---
relevant_items = []   # list of dict: {branch, act_id, title, url, why}
deadlines = []        # list of dict: {branch, act_ref, deadline_dt, url, evidence}
borderline_items = [] # list of dict

def render_section(title: str, items: list, empty_line: str) -> str:
    lines = [f"{title}", "-" * len(title)]
    if not items:
        lines.append(empty_line)
        return "\n".join(lines)
    for it in items:
        # rendering minimale, lo raffineremo dopo
        line = f"- {it.get('branch','?')} | {it.get('act_id', it.get('act_ref','?'))} | {it.get('title','(senza titolo)')}"
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

body = "\n\n".join([
    f"Monitor Parlamento — Trasporto marittimo",
    f"Generato: {now_rome} (Europe/Rome)",
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
