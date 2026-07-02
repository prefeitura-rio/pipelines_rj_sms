# -*- coding: utf-8 -*-
"""
Contratos de resultado para as tasks de extracao e consolidacao do SISREG.

ResultadoConjunto: retorno tipado de extrair_item. Encapsula os DataFrames
extraidos e as metricas de sub-itens (total/ok/ids_falhos) para que
consolidar possa derivar o status real da execucao sem depender de
argumentos literais no DAG.

Consolidado: retorno de consolidar. Carrega as tabelas prontas para upload
e as metricas completas para o log de execucao.

Separar os contratos em um modulo proprio evita dependencias circulares:
tasks.py, extractors/*.py e monitor.py podem importar daqui sem criar ciclos.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd  # noqa: F401  (usado em anotacoes de tipo)


@dataclass
class ResultadoConjunto:
    """Resultado da extracao de um conjunto de dados.

    Produzido por extrair_item (via extrator ou pelo wrapper em tasks.py) e
    consumido por consolidar. O campo ids_falhos lista os identificadores de
    sub-itens que falharam de forma recuperavel (sem PII - nunca CPFs brutos).
    """

    tabelas: Dict[str, "pd.DataFrame"]
    total: int
    ok: int
    ids_falhos: List[str] = field(default_factory=list)


@dataclass
class Consolidado:
    """Resultado da consolidacao: tabelas prontas para upload + metricas para log.

    tabelas=None indica que o gate de completude falhou ou houve erro upstream;
    o upload nao deve ocorrer e a tabela anterior permanece intacta.

    metricas contem: items_total, items_ok, linhas_por_tabela (dict),
    ids_falhos (list), status ("OK" | "FALHA_PARCIAL" | "FALHA").
    """

    tabelas: Optional[Dict[str, "pd.DataFrame"]]
    metricas: Dict[str, Any]
