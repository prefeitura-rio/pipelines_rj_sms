-- comparacao com colegas
select
    date(competencia) as competencia_avaliada,
    id_cnes,
    estabelecimento,
    profissional_cpf,
    profissional_nome,
    id_procedimento,
    procedimento,
    vagas_programadas_competencia,
    carga_horaria_ambulatorial_semanal,
    vagas_colegas_cnes_proced,
    ch_amb_colegas_cnes_proced,
    date(data_calculo_anomalia) as data_calculo
from `rj-sms.projeto_sisreg_reports.oferta_programada__anomalia_procedimento`
where
    data_calculo_anomalia = (
        select max(data_calculo_anomalia)
        from `rj-sms.projeto_sisreg_reports.oferta_programada__anomalia_procedimento`
    )
