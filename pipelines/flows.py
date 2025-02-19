# -*- coding: utf-8 -*-
# pylint: disable=W0401, W0614, W0611
# flake8: noqa: F401, F403

"""
Imports all flows for every project so we can register all of them.
"""

from pipelines.datalake.extract_load.coordenadas_estabelecimentos_pgeo3.flows import *
from pipelines.datalake.extract_load.datalake_bigquery.flows import *
from pipelines.datalake.extract_load.datasus_ftp.flows import *
from pipelines.datalake.extract_load.google_sheets.flows import *
from pipelines.datalake.extract_load.historico_clinico_integrado.flows import *
from pipelines.datalake.extract_load.minhasaude_mongodb.flows import *
from pipelines.datalake.extract_load.ser_metabase.flows import *
from pipelines.datalake.extract_load.sisreg_web.flows import *
from pipelines.datalake.extract_load.sisreg_web_v2.flows import *
from pipelines.datalake.extract_load.smsrio_mysql.flows import *
from pipelines.datalake.extract_load.tasks_clickup.flows import *
from pipelines.datalake.extract_load.tpc_azure_blob.flows import *
from pipelines.datalake.extract_load.vitacare_api.flows import *
from pipelines.datalake.extract_load.vitacare_conectividade_gcs.flows import *
from pipelines.datalake.extract_load.vitacare_db.flows import *
from pipelines.datalake.extract_load.vitai_api.flows import *
from pipelines.datalake.extract_load.vitai_db.flows import *
from pipelines.datalake.transform.dbt.flows import *
from pipelines.prontuarios.raw.smsrio.flows import *
from pipelines.reports.ingestao_dados.flows import *
from pipelines.reports.long_running_flows.flows import *
from pipelines.tools.healthchecks.flows import *
from pipelines.tools.unschedule_old_flows.flows import *
