# -*- coding: utf-8 -*-
# pylint: disable=W0401, W0614, W0611
# flake8: noqa: F401, F403

"""
Imports all flows for every project so we can register all of them.
"""

from pipelines.datalake.extract_load.datasus_ftp.flows import *
from pipelines.datalake.extract_load.google_sheets.flows import *
from pipelines.datalake.extract_load.historico_clinico_integrado.flows import *
from pipelines.datalake.extract_load.seguir_em_frente_pipefy.flows import *
from pipelines.datalake.extract_load.sih_gdrive.flows import *
from pipelines.datalake.extract_load.sisreg_web.flows import *
from pipelines.datalake.extract_load.smsrio_mysql.flows import *
from pipelines.datalake.extract_load.tpc_azure_blob.flows import *
from pipelines.datalake.extract_load.vitacare_api.flows import *
from pipelines.datalake.extract_load.vitacare_db.flows import *
from pipelines.datalake.extract_load.vitai_api.flows import *
from pipelines.datalake.extract_load.vitai_db.flows import *
from pipelines.datalake.transform.dbt.flows import *
from pipelines.prontuarios.load_datalake.flows import *
from pipelines.prontuarios.raw.smsrio.flows import *
from pipelines.reports.farmacia_digital.livro_controlados.flows import *
from pipelines.reports.ingestao_dados.flows import *
from pipelines.reports.long_running_flows.flows import *
from pipelines.tools.unschedule_old_flows.flows import *
from pipelines.tools.vitacare_healthcheck.flows import *
