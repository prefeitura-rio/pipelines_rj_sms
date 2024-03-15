# -*- coding: utf-8 -*-
# pylint: disable=W0401, W0614, W0611
# flake8: noqa: F401, F403

"""
Imports all flows for every project so we can register all of them.
"""

from pipelines.dump_api_vitacare.flows import *
from pipelines.dump_api_vitai.flows import *
from pipelines.dump_db_smsrio.flows import *
from pipelines.dump_ftp_cnes.flows import *
from pipelines.dump_url.flows import *
from pipelines.execute_dbt.flows import *
from pipelines.prontuarios.raw.smsrio.flows import *
from pipelines.prontuarios.raw.vitacare.flows import *
from pipelines.prontuarios.raw.vitai.flows import *
from pipelines.prontuarios.std.smsrio.flows import *
from pipelines.tools.api_healthcheck.flows import *
