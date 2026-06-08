# -*- coding: utf-8 -*-
"""
Taxonomia de erros do SISREG.

Cada subclasse carrega contexto estruturado (conjunto, etapa, item, url, detalhe)
para que o log de falha identifique exatamente o que quebrou sem precisar vasculhar
tracebacks. A categoria do erro guia a logica de retry/circuit-break nas
tasks genericas e nos extratores.
"""


class ErroSisreg(Exception):
    """Erro base do pacote SISREG.

    Todos os erros especificos herdam desta classe para permitir captura ampla
    quando necessario e captura especifica quando a categoria importa.
    """

    def __init__(
        self,
        mensagem: str,
        conjunto: str = "",
        etapa: str = "",
        item: str = "",
        url: str = "",
        detalhe: str = "",
    ) -> None:
        """Inicializa o erro com contexto estruturado.

        Args:
            mensagem: Descricao curta do que falhou.
            conjunto: Chave do conjunto de dados (ex.: "escalas", "afastamentos").
            etapa: Fase do pipeline onde ocorreu (ex.: "login", "extracao", "upload").
            item: Identificador do item processado (ex.: CPF mascarado, data, codigo).
            url: URL requisitada no momento da falha.
            detalhe: Informacao extra para diagnostico (sem PII).
        """
        super().__init__(mensagem)
        self.conjunto = conjunto
        self.etapa = etapa
        self.item = item
        self.url = url
        self.detalhe = detalhe

    def __str__(self) -> str:
        """Formata o erro com contexto estruturado em uma linha."""
        partes = [super().__str__()]
        if self.conjunto:
            partes.append(f"conjunto={self.conjunto}")
        if self.etapa:
            partes.append(f"etapa={self.etapa}")
        if self.item:
            partes.append(f"item={self.item}")
        if self.url:
            partes.append(f"url={self.url}")
        if self.detalhe:
            partes.append(f"detalhe={self.detalhe}")
        return " | ".join(partes)


class ErroAutenticacao(ErroSisreg):
    """Falha de autenticacao especifica de conta (credencial invalida, senha expirada).

    Escopo: conta individual. Justifica rotacao para a conta reserva.
    Nao usar para bloqueios de IP (usar ErroBloqueio).
    """


class ErroBloqueio(ErroSisreg):
    """SISREG sinalizou bloqueio de acesso: CAPTCHA, HTTP 403, 429 ou redirecionamento
    inesperado para a pagina de login.

    Escopo: IP/sessao. NAO rotacionar conta - o mesmo IP continuaria bloqueado.
    Acionar circuit-break e alertar operacao.
    """


class ErroEstrutura(ErroSisreg):
    """Estrutura HTML ou CSV inesperada: landmark ausente, tabela renomeada, campo
    oculto faltando, arquivo sem as colunas esperadas.

    Indica mudanca no SISREG. Exige investigacao humana antes de nova tentativa.
    """


class ErroVazioSuspeito(ErroSisreg):
    """Zero linhas retornadas quando o conjunto nunca e legitimamente vazio.

    Distingue ausencia benigna (procedimento sem fila) de ausencia anormal
    (0 CPFs, 0 escalas), que indica falha upstream ou mudanca silenciosa no SISREG.
    """


class ErroUpload(ErroSisreg):
    """Falha ao persistir dados no datalake (BigQuery / GCS).

    Os dados foram extraidos com sucesso mas o upload falhou. A tabela anterior
    permanece intacta (o overwrite nao e executado em caso de falha parcial).
    """


class ErroTransitorio(ErroSisreg):
    """Falha temporaria de rede ou timeout que pode se resolver com nova tentativa.

    Adequado para backoff exponencial com numero limitado de tentativas.
    Nao usar para erros estruturais ou de autenticacao.
    """
