# -*- coding: utf-8 -*-
import hashlib
import pandas as pd

from google.cloud import bigquery

# [ Conteúdo adaptado do .ipynb da Vitoria 🪦🥀 ]

# TODO: * Requisitar extração do GDB em CSVs pela API
#       * Baixar .ZIP resultante do bucket
#       * Extrair .ZIP (somente arquivos relevantes? todos?)
#       ? Apagar .ZIP do bucket?

# A partir daqui, já temos os CSVs das tabelas que precisamos

client = bigquery.Client()

gdb_to_bigquery_tables = {
    "LFCES004": "estabelecimento",
    "LFCES018": "profissional",
    "LFCES021": "vinculo",
    "LFCES038": "equipe_vinculo",
    "LFCES037": "equipe",
    "NFCES046": "equipe_tipo",
}

for gdb_table, bq_table in gdb_to_bigquery_tables.items():
    # FIXME: caminho do arquivo pós extração
    # TODO: conferir se arquivo existe, avisar caso não
    csv_path = f"/path/to/file/{gdb_table}.csv"
    df = pd.read_csv(csv_path, dtype="unicode", na_filter=False)

    # Algumas tabelas requerem um `id_profissional_sus`
    if bq_table in ( "vinculo", "profissional", "equipe_vinculo" ):
        # Vitoria que descobriu como funciona isso aqui
        df["id_profissional_sus"] = df["PROF_ID"].apply(
            lambda x: (
                hashlib.md5(x.encode("utf-8")).hexdigest()
            ).upper()[0:16]
        )

    # TODO: Garantir que o método de upload apaga as tabelas antes
    client.load_table_from_dataframe(
        df,
        f"rj-sms.brutos_cnes_gdb_staging.{bq_table}"
    )
