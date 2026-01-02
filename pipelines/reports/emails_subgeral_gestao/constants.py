from pathlib import Path

SENDER_NAME = "Secretaria Municipal de Saúde do Rio de Janeiro"
SMTP_HOST = "grid171.mailgrid.com.br"
SMTP_PORT = 587

BASE_DIR = Path(__file__).resolve().parent

LGPD_MESSAGE = \
    "Esta mensagem e seus anexos podem conter informações confidenciais, \
    de uso único e exclusivo do destinatário indicado, podendo constituir uma comunicação sigilosa. \
    O tratamento e compartilhamento do conteúdo deve ter a devida observância do que preconiza a Lei nº 13.709/2018 - \
    Lei Geral de Proteção de Dados Pessoais (LGPD), a Lei de Acesso à Informação (LAI) e as demais legislações pertinentes."