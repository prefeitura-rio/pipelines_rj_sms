import os

# Lê um arquivo html que está no caminho absoluto especificado
def read_html_file(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()

flow_params = {
    "recipients": ["matheusmiloski.smsrio@gmail.com", "juliana.paranhos@regulacaoriorj.com.br"],
    "subject": "Teste de envio via smtplib com anexos",
    "html_body": read_html_file(os.path.join(os.path.dirname(__file__), "teste.html")),
    "plain_body": "Mensagem de teste do envio via smtplib.",
    "attachments": [
        # Exemplo: r"C:\temp\relatorio.csv"

        os.path.join(os.path.dirname(__file__), "arquivo1.csv"),
        os.path.join(os.path.dirname(__file__), "arquivo2.csv")
    ],
    "inline_images": {
        # Exemplo "logo": r"C:\temp\logo.png"
    },
}
