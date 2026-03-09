import logging
import os
import re
import smtplib
from datetime import datetime
from email.message import EmailMessage
from typing import Any, Dict, List, Tuple

import yaml

from senato_sparql import fetch_recent_ddl, fetch_recent_sindisp

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)


def normalize_text(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"[^\w\sàèéìòóù]", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def _collect_lists(node: Any, path: Tuple[str, ...] = ()) -> List[Tuple[Tuple[str, ...], List[str]]]:
    out: List[Tuple[Tuple[str, ...], List[str]]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            out.extend(_collect_lists(value, path + (str(key).lower(),)))
    elif isinstance(node, list):
        strings = [str(x).strip() for x in node if str(x).strip()]
        if strings:
            out.append((path, strings))
    return out


def load_kb(path: str = "kb.yaml") -> Dict[str, List[str]]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    include, exclude = [], []
    for path_tokens, values in _collect_lists(data):
        joined = " ".join(path_tokens)
        if any(k in joined for k in ("exclude", "negative", "non", "irrilev", "not_relevant", "fuori")):
            exclude.extend(values)
        elif any(k in joined for k in ("include", "positive", "settore", "maritt", "shipping", "relevant", "rilev")):
            include.extend(values)

    include = sorted(set(normalize_text(x) for x in include if x))
    exclude = sorted(set(normalize_text(x) for x in exclude if x))
    return {"include": include, "exclude": exclude}


def classify_item(item: Dict[str, str], kb: Dict[str, List[str]]):
    haystack = normalize_text(
        " ".join(
            [
                item.get("categoria_atto", ""),
                item.get("tipo", ""),
                item.get("numero", ""),
                item.get("a_chi", ""),
                item.get("proponenti", ""),
                item.get("gruppo", ""),
                item.get("titolo", ""),
                item.get("testo", ""),
            ]
        )
    )

    include_hits = [kw for kw in kb.get("include", []) if kw and kw in haystack]
    exclude_hits = [kw for kw in kb.get("exclude", []) if kw and kw in haystack]

    score = len(include_hits) - len(exclude_hits)
    if include_hits and not exclude_hits:
        return True, {"include_hits": include_hits, "exclude_hits": exclude_hits}
    if exclude_hits and not include_hits:
        return False, {"include_hits": include_hits, "exclude_hits": exclude_hits}
    if score > 0:
        return True, {"include_hits": include_hits, "exclude_hits": exclude_hits}
    return False, {"include_hits": include_hits, "exclude_hits": exclude_hits}


def format_line(idx: int, item: Dict[str, str]) -> str:
    parts = [
        f"[{idx}]",
        item.get("fonte", "-"),
        item.get("tipo", "-"),
        f"Numero: {item.get('numero', '-')}",
        f"A chi è rivolta: {item.get('a_chi', '-')}",
        f"Proponente/i: {item.get('proponenti', '-')}",
        f"Gruppo parlamentare: {item.get('gruppo', '-')}",
        f"Stato: {item.get('stato', '-')}",
        f"Link: {item.get('link', '-')}",
    ]
    if item.get("categoria_atto") == "DDL":
        parts.insert(3, f"Titolo: {item.get('titolo', '-')}")
    return " | ".join(parts)


def build_report(days_back: int = 7, legislatura: str = "19"):
    kb = load_kb("kb.yaml")

    ddl_items = fetch_recent_ddl(days_back=days_back, legislatura=legislatura)
    sindisp_items = fetch_recent_sindisp(days_back=days_back, legislatura=legislatura)

    LOG.info("DDL trovati dopo filtro Python: %s", len(ddl_items))
    LOG.info("Sindisp trovati dopo filtro Python: %s", len(sindisp_items))

    all_items = ddl_items + sindisp_items

    relevant, non_relevant = [], []
    for item in all_items:
        is_relevant, debug = classify_item(item, kb)
        item["debug_include_hits"] = ", ".join(debug["include_hits"]) or "-"
        item["debug_exclude_hits"] = ", ".join(debug["exclude_hits"]) or "-"
        (relevant if is_relevant else non_relevant).append(item)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    subject = f"Monitor Parlamento — Trasporto marittimo ({generated_at})"

    lines: List[str] = []
    lines.append("Monitor Parlamento — Trasporto marittimo")
    lines.append(f"Generato: {generated_at} (Europe/Rome)")
    lines.append("")
    lines.append("Sorgenti / Warning")
    lines.append("------------------")
    lines.append(f"- DEBUG DDL trovati dopo filtro Python: {len(ddl_items)}")
    lines.append(f"- DEBUG Sindisp trovati dopo filtro Python: {len(sindisp_items)}")
    lines.append("")
    lines.append("Riguarda il settore")
    lines.append("-------------------")
    if relevant:
        for idx, item in enumerate(relevant, start=1):
            lines.append(format_line(idx, item))
    else:
        lines.append("Nessun atto rilevante trovato.")
    lines.append("")
    lines.append("Non riguarda il settore")
    lines.append("-----------------------")
    if non_relevant:
        for idx, item in enumerate(non_relevant, start=1):
            lines.append(format_line(idx, item))
    else:
        lines.append("Nessun atto non rilevante trovato.")

    return subject, "\n".join(lines)


def send_email(subject: str, body: str) -> None:
    host = os.environ.get("SMTP_HOST") or os.environ.get("SMTP_SERVER")
    if not host:
        raise RuntimeError("Manca SMTP_HOST (o SMTP_SERVER) nelle variabili d'ambiente.")

    port = int(os.environ.get("SMTP_PORT", os.environ.get("SMTP_SERVER_PORT", "587")))
    username = os.environ["SMTP_USERNAME"]
    password = os.environ["SMTP_PASSWORD"]
    to_email = os.environ["ALERT_TO_EMAIL"]
    from_email = os.environ.get("SMTP_FROM_EMAIL", username)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email
    msg.set_content(body)

    with smtplib.SMTP(host, port, timeout=60) as server:
        server.starttls()
        server.login(username, password)
        server.send_message(msg)


if __name__ == "__main__":
    days_back = int(os.environ.get("DAYS_BACK", "7"))
    legislatura = os.environ.get("LEGISLATURA", "19")
    subject, body = build_report(days_back=days_back, legislatura=legislatura)
    print(body)
    send_email(subject, body)
