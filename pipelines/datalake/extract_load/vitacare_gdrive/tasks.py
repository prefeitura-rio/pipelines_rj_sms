from pipelines.utils.credential_injector import authenticated_task as task


@task
def log_folder_structure(files: list, environment: str):
    print(f"Environment: {environment}")
    for file in files:
        print(f"File: {file['name']}")

@task
def filter_files_by_date(files: list, reference_month: str):
    return []

