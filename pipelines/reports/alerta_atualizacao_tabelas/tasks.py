# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests
from google.cloud import bigquery

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.monitor import send_message


@task
def verify_tables_freshness(environment: str, table_ids: dict):
    results = []
    for table_id, projects in table_ids.items():
        table_metadata = bigquery.Client().get_table(table_id)

        today_americas = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

        last_updated = (
            table_metadata.modified
            if table_metadata.modified is not None
            else table_metadata.created
        )
        last_updated_americas = last_updated.astimezone(pytz.timezone("America/Sao_Paulo"))

        results.append(
            {
                "table_id": table_id,
                "last_updated": last_updated_americas,
                "was_updated_today": last_updated_americas.date() == today_americas.date(),
                "affected_projects": projects,
            }
        )

    return results


@task
def send_discord_alert(environment: str, results: list):
    lines = []
    affected_projects = []
    for result in results:
        if result["was_updated_today"]:
            continue

        affected_projects.extend(result["affected_projects"])

        last_updated = result["last_updated"].astimezone(pytz.timezone("America/Sao_Paulo"))
        lines.append(
            f"- 🔴 `{result['table_id']}` *Última Modificação: {last_updated.strftime('%d/%m/%Y %H:%M:%S')}*"
        )

    if len(lines) == 0:
        send_message(
            title="Alerta de Tabelas Desatualizadas",
            message="🟢 Todas as tabelas estão atualizadas",
            monitor_slug="freshness",
            suppress_embeds=True,
        )
        return

    affected_projects = list(set(affected_projects))
    affected_projects = [f"- {project}" for project in affected_projects]

    message = "**Houve interrupção na atualização das tabelas:**\n" + "\n".join(lines)
    message += "\n\n**Projetos Afetados:**\n" + "\n".join(affected_projects)

    send_message(
        title="Alerta de Tabelas Desatualizadas",
        message=message,
        monitor_slug="freshness",
        suppress_embeds=True,
    )

@task 
def verify_hci_last_episodes(environment: str) -> list:
    sql = """
        select  
            provider,
            max(entry_datetime) as last_episode
        from `rj-sms.app_historico_clinico.episodio_assistencial` 
        group by provider
    """
    df = bigquery.Client().query(sql).to_dataframe()
    df['last_episode'] = pd.to_datetime(df['last_episode'])
    return [dict(df.iloc[row]) for row in range(0, len(df))]

@task 
def send_hci_discord_alert(environment: str, last_episodes: list):
    lines = []
    
    for record in last_episodes:
        provider = record['provider']
        last_episode = pytz.timezone("America/Sao_Paulo").localize(record['last_episode'])
        days_diff = (datetime.now(pytz.timezone("America/Sao_Paulo")) - last_episode).days
        
        if days_diff > 1:
            lines.append(
                f"- 🔴 `{provider}` *Último Episódio: {last_episode.strftime('%d/%m/%Y %H:%M:%S')}* ({days_diff} dias atrás)"
            )
        else: 
            lines.append(
                f"- 🟢 `{provider}` *Último Episódio: {last_episode.strftime('%d/%m/%Y %H:%M:%S')}* ({days_diff} dias atrás)"
            )
        
    send_message(
        title="Alerta de Últimos Episódios Assistenciais - HCI",
        message="\n".join(lines),
        monitor_slug="freshness",
        suppress_embeds=True,
    )
    