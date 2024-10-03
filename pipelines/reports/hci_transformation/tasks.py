import subprocess
import glob

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import get_secret
from pipelines.utils.tasks import load_file_from_bigquery


@task
def get_diagram_template(
    entity: str,
    environment: str,
) -> str:
    secret_name = f"REPORT_INGESTION_TEMPLATE_{entity.upper()}"
    
    try:
        template = get_secret(
            secret_name=secret_name,
            environment=environment,
        )[secret_name]
    except Exception as e:
        raise Exception(
            f"Template not found for entity '{entity}' in environment '{environment}'."
        )
    return template

@task
def calculate_data(
    entity: str,
    environment: str,
) -> dict:
    statistics = load_file_from_bigquery.run(
        project_name="rj-sms",
        dataset_name="gerenciamento__historico_clinico",
        table_name="processamento_estatisticas",
        environment=environment
    )
    entity_statistics = statistics[statistics["entidade"] == "episodio"] # TMP

    data = {}
    for source in ['vitacare', 'vitai', 'smsrio']:
        source_statistics = entity_statistics[entity_statistics["prontuario"] == source].to_dict(orient="records")

        if len(source_statistics) == 0:
            break

        source_statistics = source_statistics[0]
        data[f"{source}_raw"] = source_statistics["raw"]
        data[f"{source}_int"] = source_statistics["int"]
        data[f"{source}_mrg"] = source_statistics["mrg"]
        data[f"{source}_app"] = source_statistics["app"]

    return data

@task
def generate_diagram(
    diagram_template: str,
    data: dict,
    target_date: str,
) -> None:

    # Write diagram content to file
    with open("diagram.md", "w") as f:
        f.write(
            diagram_template.format(**data)
        )

    # Generate diagram
    process = subprocess.Popen(
        ' '.join([
            "mmdc",
            "-i", "./diagram.md", 
            "-o", "./output.png", 
            "-s", "1.5", 
            "-b", "transparent", 
            "-t", "dark"
        ]),
        shell=True
    )
    process.wait()

    # Get the diagram file
    diagram_file = glob.glob("*.png")[0]

    send_message(
        title=f"Relatório de Ingestão de Dados ({target_date})",
        message="",
        file_path=diagram_file,
        monitor_slug="data-ingestion"
    )

