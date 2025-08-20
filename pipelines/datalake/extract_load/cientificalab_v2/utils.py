

def garantir_lista(item):
    """Garante que o item de entrada seja sempre uma lista para iteração segura."""
    if item is None:
        return []
    if isinstance(item, list):
        return item
    return [item]


def achatar_dicionario(dicionario_aninhado: dict, prefixo: str) -> dict:
    """
    'Achata' um dicionário aninhado, adicionando um prefixo
    Ex: {'nome': 'Ana'}, 'paciente' -> {'paciente_nome': 'Ana'}
    """
    if not isinstance(dicionario_aninhado, dict):
        return {}

    return {f"{prefixo}_{chave}": valor for chave, valor in dicionario_aninhado.items()}