select
    protocolo_id,
    data_solicitacao
from `rj-sms.brutos_siscan_web.laudos_mamografia`
tablesample system (1 percent)