from typing import Any, Dict, List


def ensure_list(obj: Any) -> List[Dict]:
    """
    Garante que o objeto seja uma lista.
    Se for None ou vazio, retorna lista vazia.
    Se for dict, transforma em lista de um elemento.
    """
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        return [obj]
    return []


def flatten_dict(d: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    """
    "Achata" um dicionário, prefixando suas chaves.
    """
    return {f"{prefix}_{k}": v for k, v in d.items()}