import os
import smtplib
from email.message import EmailMessage

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

username = os.environ["SMTP_USERNAME"]
password = os.environ["SMTP_PASSWORD"]
to_email = os.environ["ALERT_TO_EMAIL"]

msg = EmailMessage()
msg["Subject"] = "Test GitHub Actions: email OK"
msg["From"] = username
msg["To"] = to_email
msg.set_content("Se leggi questa email, l'invio da GitHub Actions via Gmail SMTP funziona.")

with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
    s.ehlo()
    s.starttls()
    s.login(username, password)
    s.send_message(msg)

print("Email inviata a", to_email)
