import json
import re
from datetime import timedelta
import pandas as pd
import requests
from pipelines.utils.credential_injector import authenticated_task as task

@task(nout=3)
def reduce_raw_column(df: pd.DataFrame):
    desc_interna = df["procedimento"].fillna("").astype(str).values
    desc_grupo = df["procedimento_grupo"].fillna("").astype(str).values
    codigos = df["id_procedimento_sisreg"].fillna("").astype(str).values
    return desc_interna, desc_grupo, codigos

@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_result_gemini(procedimento: str, procedimento_grupo: str, id_procedimento_sisreg: str, gemini_key: str, model: str) -> dict:
    prompt = f"""
    Você é responsável por padronizar nomes de procedimentos médicos para que sejam claros e fáceis de entender por pacientes.
    Regras para Padronização (em ordem de prioridade):
    Privacidade do Paciente (Regra Fundamental): NUNCA inclua informações sobre diagnósticos ou condições sensíveis dos pacientes (como HIV, câncer, etc.). Esta regra anula qualquer outra.
    Foco no Procedimento, Não na Aplicação: Diferencie o nome do procedimento dos detalhes da sua aplicação. Remova especificadores que variam de paciente para paciente, como números de dentes, ou características específicas do paciente. O nome padronizado deve representar o procedimento em si, não a sua instância particular.
    Qualificacao do alvo: Remova adjetivos que descrevem o estado ou fase da estrutura anatômica e termos tecnicos que o paciente possa nao compreender
    Lógica de Combinação Inteligente:
    a. Extrair a Categoria Base: A partir do procedimento_grupo, extraia o nome completo da categoria. Para isso, remova apenas prefixos genéricos como "GRUPO" e mantenha o restante da descrição. Extrair a Especificação: A partir do procedimento, identifique o termo que descreve a técnica ou variação específica que complementa a categoria base
    c. Combinar sem Redundância: Junte a Categoria Base (a) com a Especificação (b) na ordem [Categoria Base] [Especificação]. Assegure-se de que não haja repetição de palavras entre as duas partes.
    Limpeza e Formatação Final:
    a. Remoção de Jargões: Elimine siglas e termos técnicos desnecessários que não foram usados na combinação.
    b. Normalização de Termos: Corrija e padronize a grafia de termos para o português correto
    c. Formato: O nome do procedimento_padronizado deve estar em Caixa Título (primeira letra de cada palavra em maiúscula, exceto preposições e artigos curtos).
    Você receberá dois dados de entrada: procedimento_grupo e procedimento. Sua tarefa é criar um nome padronizado, único e otimizado para o procedimento.
    Entrada:
        Descrição interna: "{procedimento}"
        Descrição Grupo: "{procedimento_grupo}"


    Crie um nome padronizado para o procedimento usando as informações fornecidas e retorne APENAS o id_procedimento_sisreg e o procedimento_padronizado em formato JSON na resposta final da seguinte forma:
    Formato de saída esperado. Retorne APENAS isso:
    {{
        "id_procedimento_sisreg": "{id_procedimento_sisreg}",
        "procedimento_padronizado": "<texto_padronizado>"
    }}
    """

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}"
    headers = {"Content-Type": "application/json"}
    data = {"contents": [{"parts": [{"text": prompt}]}]}

    response = requests.post(url, data=json.dumps(data), headers=headers)

    print("=== DEBUG Gemini API ===")
    print(f"Status code: {response.status_code}")
    try:
        print("Raw response:", response.text[:1000]) 
    except Exception:
        pass

    if response.status_code == 200:
        try:
            payload = response.json()
            raw_text = payload["candidates"][0]["content"]["parts"][0]["text"]
            print("Raw text from Gemini:", raw_text)

            cleaned = re.sub(r"^```(?:json)?\s*", "", raw_text.strip(), flags=re.IGNORECASE | re.MULTILINE)
            cleaned = re.sub(r"\s*```$", "", cleaned.strip(), flags=re.MULTILINE)
            print("Cleaned text:", cleaned)

            parsed = json.loads(cleaned)
            print("Parsed JSON:", parsed)

            orig_id = str(id_procedimento_sisreg).strip()
            pad = parsed.get("procedimento_padronizado", None)
            if isinstance(pad, str):
                pad = pad.strip()
                if pad == "":
                    pad = None

            result = {
                "id_procedimento_sisreg": orig_id,
                "procedimento_padronizado": pad
            }
            print("Final result:", result)
        except Exception as e:
            print("Parsing error:", e)
            result = {
                "id_procedimento_sisreg": str(id_procedimento_sisreg).strip(),
                "procedimento_padronizado": None
            }
    else:
        raise ValueError(f"API call failed, error: {response.status_code} - {response.reason}")

    return result

@task
def parse_result_dataframe(list_result: list) -> pd.DataFrame:
    df = pd.DataFrame(list_result)
    return df