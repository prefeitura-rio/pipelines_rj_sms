# -*- coding: utf-8 -*-
# pylint: disable=W0401, W0614, W0611
# flake8: noqa: F401, F403

"""
Imports all flows for every project so we can register all of them.
"""

# ===============================
# EXTRACT AND LOAD
# ===============================
from pipelines.datalake.extract_load.centralregulacao_mysql.flows import *
from pipelines.datalake.extract_load.cientificalab_api.flows import *
from pipelines.datalake.extract_load.coordenadas_estabelecimentos_pgeo3.flows import *
from pipelines.datalake.extract_load.datalake_bigquery.flows import *
from pipelines.datalake.extract_load.datasus_ftp.flows import *
from pipelines.datalake.extract_load.diario_oficial_rj.flows import *
from pipelines.datalake.extract_load.diario_oficial_uniao.flows import *
from pipelines.datalake.extract_load.diario_oficial_uniao_api.flows import *
from pipelines.datalake.extract_load.gal_gcs.flows import *
from pipelines.datalake.extract_load.google_sheets.flows import *
from pipelines.datalake.extract_load.medilab_api.flows import *
from pipelines.datalake.extract_load.minhasaude_mongodb.flows import *
from pipelines.datalake.extract_load.relational_db.flows import *
from pipelines.datalake.extract_load.ser_metabase.flows import *
from pipelines.datalake.extract_load.siscan_web_laudos.flows import *
from pipelines.datalake.extract_load.sisreg_api.flows import *
from pipelines.datalake.extract_load.sisreg_web.flows import *
from pipelines.datalake.extract_load.sisreg_web_v2.flows import *
from pipelines.datalake.extract_load.smsrio_mysql.flows import *
from pipelines.datalake.extract_load.subpav_mysql.flows import *
from pipelines.datalake.extract_load.tpc_azure_blob.flows import *
from pipelines.datalake.extract_load.tribunal_de_contas_rj.flows import *
from pipelines.datalake.extract_load.vitacare_api_v2.flows import *
from pipelines.datalake.extract_load.vitacare_gdrive.flows import *
from pipelines.datalake.extract_load.vitacare_historico.flows import *
from pipelines.datalake.extract_load.vitai_api.flows import *
from pipelines.datalake.extract_load.vitai_db.flows import *

# ===============================
# MIGRATE
# ===============================
from pipelines.datalake.migrate.gcs_to_cloudsql.flows import *
from pipelines.datalake.migrate.gdrive_to_gcs.flows import *
from pipelines.datalake.migrate.orquestracao_cdi.flows import *

# ===============================
# TRANSFORM
# ===============================
from pipelines.datalake.transform.dbt.flows import *

# ===============================
# GEMINI
# ===============================
from pipelines.datalake.transform.gemini.pacientes_restritos.flows import *
from pipelines.reports.alerta_atualizacao_tabelas.flows import *
from pipelines.reports.alerta_jobs_caros.flows import *
from pipelines.reports.checks_bucket_files.flows import *
from pipelines.reports.informes_seguranca.flows import *

# ===============================
# REPORTS
# ===============================
from pipelines.reports.ingestao_dados.flows import *
from pipelines.reports.long_running_flows.flows import *
from pipelines.reports.monitoramento_hci.flows import *

# ===============================
# TOOLS
# ===============================
from pipelines.tools.healthchecks.flows import *
from pipelines.tools.unschedule_old_flows.flows import *
