
import requests

# ─── 1) preencha aqui os cookies que capturou ──────────────────────────────
COOKIES = {
    "SESSION": "b0b465c64b622defe5b489b75970c1297d32f4182037f0db6b242190ff35b0a7",
    "ID":      "512735",
    "TS01993b5d": "0140e3e4e5490cf8c84f0bbc2b83e62b2c5ed868d91d35766ff7a855c82cf68b9215497d38d6bd7db82bf820579048f22a4b75e48d",
}

DOMAIN = "sisregiii.saude.gov.br"
URL_PROTECTED = f"https://{DOMAIN}/cgi-bin/index"

# ─── 2) cria a sessão com esses cookies ────────────────────────────────────
sess = requests.Session()
for nome, valor in COOKIES.items():
    sess.cookies.set(nome, valor, domain=DOMAIN, path="/")

# ─── 3) tenta acessar uma página que só carrega se estiver logado ──────────
resp = sess.get("https://sisregiii.saude.gov.br/cgi-bin/index#", allow_redirects=True, timeout=30)

# heurística: se foi parar numa URL de login ou o HTML ainda tem "usuario/senha", falhou
login_like = (
    "login" in resp.url.lower()
    or ("usuario" in resp.text.lower() and "senha" in resp.text.lower())
)

if login_like:
    print("❌ cookies inválidos ou sessão expirada — faça login de novo")
else:
    print("✅ cookies válidos — você está autenticado!")
