# -*- coding: utf-8 -*-
# pylint: disable=W0401, W0614
# flake8: noqa: F401, F403

"""
Imports all flows for every project so we can register all of them.
"""

from pipelines.dump_api_vitai.flows import *
from pipelines.execute_dbt.flows import *
from pipelines.prontuarios.raw.smsrio.flows import *
from pipelines.prontuarios.raw.vitai.flows import *