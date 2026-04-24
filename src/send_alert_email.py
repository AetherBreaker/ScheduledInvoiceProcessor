from __future__ import annotations

import smtplib
import ssl
from email.message import EmailMessage
from logging import getLogger

from environment_init_vars import SETTINGS

ALERTS_EMAIL = SETTINGS.alerts_email
ALERTS_EMAIL_PWD = SETTINGS.alerts_email_pwd


ALERTS_RECIPIENTS = SETTINGS.alerts_recipients

logger = getLogger(__name__)


def send_alert_email(subject: str, content: str) -> None:
  if not ALERTS_RECIPIENTS:
    logger.warning("Skipping alert email because no recipients are configured.")
    return

  msg = EmailMessage()
  msg.set_content(content)
  msg["Subject"] = subject
  msg["From"] = ALERTS_EMAIL
  msg["To"] = ", ".join([str(recipient) for recipient in ALERTS_RECIPIENTS])
  context = ssl.create_default_context()
  try:
    with smtplib.SMTP("smtppro.zoho.com", 587) as server:
      server.ehlo()
      server.starttls(context=context)
      server.ehlo()
      server.login(ALERTS_EMAIL, ALERTS_EMAIL_PWD)
      server.send_message(msg)
    logger.debug("Alert email sent successfully.")
  except Exception:
    logger.error("Failed to send alert email.", exc_info=True)
