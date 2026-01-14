# -*- coding: utf-8 -*-
from typing import List, Optional

from prefeitura_rio.pipelines_utils.custom import Flow as CustomFlow


class Flow(CustomFlow):
    def __init__(self, *args, owners: Optional[List[str]] = None, **kwargs):
        # Guarda a lista de owners como atributo
        self.owners = owners or []

        # Continua chamando o __init__ do Flow customizado original
        super().__init__(*args, **kwargs)

    def get_owners(self):
        return self.owners
