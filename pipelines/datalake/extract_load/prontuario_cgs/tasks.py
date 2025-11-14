import os
import tarfile
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log 
from pipelines.utils.googleutils import download_from_cloud_storage


@task
def get_file(path, bucket_name, blob_prefix, environment):
    log('⬇️  Realizando download do arquivo...')
    return download_from_cloud_storage(path, bucket_name, blob_prefix)
    
@task
def unpack_files(tar_files:str, output_dir:str):
    log('📁 Descompactando os arquivos...')
    
    outputs = []
    for file in tar_files:
        output_path = os.path.join(output_dir, os.path.basename(file).replace('.tar.gz', ''))
        with tarfile.open(file, "r:gz") as tar:
            tar.extractall(path=output_path)
            outputs.append(output_path)
    return outputs

@task
def print_log(message):
    log(message)