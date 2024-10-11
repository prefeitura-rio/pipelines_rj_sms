# -*- coding: utf-8 -*-
import glob
import subprocess

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import get_secret, load_file_from_bigquery
from pipelines.constants import constants


@task
def get_data(environment, target_date):

    data = load_file_from_bigquery.run(
        project_name=constants.GOOGLE_CLOUD_PROJECT.value[environment],
        dataset_name='gerenciamento__monitoramento',
        table_name='estatisticas_farmacia',
        environment=environment,
    )

    if not target_date:
        target_date = data['data_atualizacao'].max()

    return data[data['data_atualizacao'] == target_date]


@task
def send_report(data, target_date):

    if data.empty:
        log("No data to report")
        return

    message_lines = []
    for type in data['tipo'].unique():
        message_lines.append(f"## {type.capitalize()} de Estoque")

        data_subset = data[data['tipo'] == type]

        for fonte in data_subset['fonte'].unique():
            fonte_data = data_subset[data_subset['fonte'] == fonte]
            fonte_data = fonte_data.to_dict(orient='records')[0]

            title = fonte.upper()
            units_with_data = fonte_data['quant_unidades_com_dado']
            message_lines.append(f"### **{title}**")
            message_lines.append(f"- 📊 **Unidades com dados:** {units_with_data}")
            message_lines.append( "- 🚨 **Unidades sem dados:**")

            units_without_data = sorted(fonte_data['unidades_sem_dado'], key=lambda x: x['unidade_ap'])
            
            for unit in units_without_data:
                ap = unit['unidade_ap']
                name = unit['unidade_nome']
                cnes = unit['unidade_cnes']
                message_lines.append(f"> `[AP{ap}] {name} ({cnes})`") # noqa

    send_message(
        title=f"Farmácia Digital - Monitoramento de Ingestão de Dados ({target_date})",
        message='\n'.join(message_lines),
        monitor_slug="data-ingestion"
    )