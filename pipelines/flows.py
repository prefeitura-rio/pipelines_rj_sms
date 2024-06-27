# -*- coding: utf-8 -*-
# pylint: disable=W0401, W0614, W0611
# flake8: noqa: F401, F403

"""
Imports all flows for every project so we can register all of them.
"""

from pipelines.datalake.extract_load.datasus_ftp.flows import *
from pipelines.datalake.extract_load.google_sheets.flows import *
from pipelines.datalake.extract_load.historico_clinico_integrado.flows import *
from pipelines.datalake.extract_load.sisreg_web.flows import *
from pipelines.datalake.extract_load.smsrio_mysql.flows import *
from pipelines.datalake.extract_load.tpc_azure_blob.flows import *
from pipelines.datalake.extract_load.vitacare_api.flows import *
from pipelines.datalake.extract_load.vitai_api.flows import *
from pipelines.datalake.transform.dbt.flows import *
from pipelines.misc.historical_mrg.flows import *
from pipelines.prontuarios.load_datalake.flows import *
from pipelines.prontuarios.mrg.flows import *
from pipelines.prontuarios.raw.smsrio.flows import *
from pipelines.prontuarios.raw.vitai.flows import *
from pipelines.prontuarios.std.smsrio.flows import *
from pipelines.prontuarios.std.vitacare.flows import *
from pipelines.prontuarios.std.vitai.flows import *
from pipelines.reports.data_ingestion.flows import *
from pipelines.reports.endpoint_health.flows import *
from pipelines.tools.api_healthcheck.flows import *
from pipelines.tools.unschedule_old_flows.flows import *
from pipelines.tools.vitacare_healthcheck.flows import *
