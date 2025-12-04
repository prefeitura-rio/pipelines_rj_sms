import os
from dotenv import load_dotenv
load_dotenv()

SMTP_HOST = "grid171.mailgrid.com.br"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SENDER_EMAIL = SMTP_USER
SENDER_NAME = "Secretaria Municipal de Saúde do Rio de Janeiro"
