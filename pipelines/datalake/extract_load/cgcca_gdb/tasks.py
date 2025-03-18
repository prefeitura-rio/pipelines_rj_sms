from pipelines.utils.credential_injector import authenticated_task as task

import fdb


@task
def extract_cgcca_gdb():
    con = fdb.connect(dsn='localhost:./CNES_05072024.gdb')
    cur = con.cursor()
    pass