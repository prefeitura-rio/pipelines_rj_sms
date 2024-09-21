# -*- coding: utf-8 -*-
"""
Functions to generate the report
"""
import os

import pandas as pd
from docxtpl import DocxTemplate


def gerar_dados_com_movimentacao(df: pd.DataFrame) -> list:
    """
    Generate data for medications with movement records.

    Args:
        df (pd.DataFrame): DataFrame containing medication movement data.

    Returns:
        list: A list of dictionaries containing processed data for each medication with movement.
    """

    pairs_cnes_material = (
        df[
            [
                "id_cnes",
                "id_material",
                "nome",
                "controlado_tipo",
            ]
        ]
        .drop_duplicates()
        .to_dict(orient="records")
    )

    dados = []

    for pair in pairs_cnes_material:
        df2 = df[(df["id_cnes"] == pair["id_cnes"]) & (df["id_material"] == pair["id_material"])][
            [
                "tipo_evento",
                "data_evento_br",
                "evento",
                "movimento_quantidade",
                "posicao_final",
                "id_lote",
                "data_validade_br",
                "ordem",
            ]
        ].copy()

        records = df2.to_dict(orient="records")

        dados.append(
            {
                "id_cnes": pair["id_cnes"],
                "id_material": pair["id_material"],
                "material_nome": f'{pair["nome"]} - {pair["id_material"]}',
                "controlado_tipo": pair["controlado_tipo"],
                "saldo_inicial": df2.iloc[0].posicao_final - df2.iloc[0].movimento_quantidade,
                "saldo_final": df2.iloc[-1].posicao_final,
                "soma_entradas": df2[df2["tipo_evento"] == "entrada"]["movimento_quantidade"].sum(),
                "soma_saidas": df2[df2["tipo_evento"] == "saída"]["movimento_quantidade"].sum(),
                "records": records,
            }
        )
    return dados


def gerar_dados_sem_movimentacao(df: pd.DataFrame) -> list:
    """
    Generate data for medications without movement records.

    Args:
        df (pd.DataFrame): DataFrame containing medication data without movement records.

    Returns:
        list: A list of dictionaries containing processed data for each medication without movement.
    """
    data = df.to_dict(orient="records")

    for item in data:
        item.update(
            {"records": [], "material_nome": f'{item["material_nome"]} - {item["id_material"]}'}
        )

    return data


def gerar_dados(df_com_mov: pd.DataFrame, df_sem_mov: pd.DataFrame) -> list:
    """
    Generate data for medications with or without movement records.

    Args:
        df_com_mov (pd.DataFrame): DataFrame containing medication movement data.
        df_sem_mov (pd.DataFrame): DataFrame containing medication data without movement records.

    Returns:
        list: A list of dictionaries containing processed data for each medication.
    """

    dados = gerar_dados_com_movimentacao(df_com_mov) + gerar_dados_sem_movimentacao(df_sem_mov)

    return sorted(dados, key=lambda x: x["material_nome"])


def gerar_relatorio(
    template: str,
    output_directory: str,
    payload: dict,
):
    """
    Generate a report for a given payload.

    Args:
        template (str): Path to the template file.
        output_directory (str): Directory to save the generated report.
        payload (dict): Dictionary containing data for the report.
    """
    # Carregar o template do documento
    doc = DocxTemplate(template)

    # Preparar os dados para o template
    context = {
        "nome_unidade": payload["nome_unidade"],
        "endereco": payload["endereco"],
        "controlado_tipo": payload["controlado_tipo"],
        "farmaceuticos": payload["farmaceuticos"],
        "competencia_inicio": payload["competencia_inicio"],
        "competencia_fim": payload["competencia_fim"],
        "medicamentos": payload["medicamentos"],
    }

    # Renderizar o documento com os dados
    doc.render(context)

    # Salvar o documento gerado
    unidade = payload["nome_unidade"].replace(" ", "_").lower()
    competencia = f"20{payload['competencia_inicio'][-2:]}-{payload['competencia_inicio'][3:5]}"
    controlado = payload["controlado_tipo"]

    doc.save(f"{output_directory}/{unidade}__{competencia}__{controlado}.docx")


def gerar_relatorios(
    df_relacao: pd.DataFrame,
    df_com_mov: pd.DataFrame,
    df_sem_mov: pd.DataFrame,
    base_directory: str,
    competencia_inicio: str,
    competencia_fim: str,
    competencia: str,
    template: str,
):
    """
    Generate reports for each record in the given DataFrame.

    Args:
        df_relacao (pd.DataFrame): DataFrame containing all reports that will be generated.
        df_com_mov (pd.DataFrame): DataFrame containing medication movement data.
        df_sem_mov (pd.DataFrame): DataFrame containing medication data without movement records.
        base_directory (str): Base directory to save the generated reports.
        competencia_inicio (str): Start date of the competition period.
        competencia_fim (str): End date of the competition period.
        competencia (str): Competition period.
        template (str): Path to the template file.
    """

    report_relacao = df_relacao.to_dict(orient="records")

    for report in report_relacao:

        output_directory = f"{base_directory}/{report['estabelecimento_area_programatica']}/{report['estabelecimento_nome']}/{competencia}"  # noqa: E501

        # Create partition directory
        if not os.path.exists(output_directory):
            os.makedirs(output_directory, exist_ok=False)

        medicamentos = gerar_dados(
            df_com_mov=df_com_mov[
                (df_com_mov["id_cnes"] == report["id_cnes"])
                & (df_com_mov["controlado_tipo"] == report["controlado_tipo"])
            ],
            df_sem_mov=df_sem_mov[
                (df_sem_mov["id_cnes"] == report["id_cnes"])
                & (df_sem_mov["controlado_tipo"] == report["controlado_tipo"])
            ],
        )

        payload = {
            "nome_unidade": report["estabelecimento_nome"],
            "endereco": report["estabelecimento_endereco"],
            "controlado_tipo": report["controlado_tipo"],
            "farmaceuticos": report["farmaceutico"],
            "competencia_inicio": competencia_inicio,
            "competencia_fim": competencia_fim,
            "medicamentos": medicamentos,
        }

        gerar_relatorio(
            template=template,
            output_directory=output_directory,
            payload=payload,
        )
