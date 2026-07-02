# -*- coding: utf-8 -*-
"""
Nucleo do pipeline SISREG: tudo que nao e a superficie Prefect.

Contem a camada de acesso ao SISREG (auth, http, parsing), os contratos
(errors, resultado), o registry de conjuntos, o monitor de frescor e os
extratores por conjunto. A superficie Prefect (flows, tasks, schedules,
constants) fica na raiz do pacote sisreg/.
"""
