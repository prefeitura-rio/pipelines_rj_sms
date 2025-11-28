"""
Constants
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitacare flows
    """
    DOWNLOAD_DIR = './temp'
    UNCOMPRESS_FILES_DIR = './data'
    UPLOAD_PATH = './upload'
    DICTIONARY_ENCODING = 'ISO-8859-1'
    SELECTED_OPENBASE_TABLES = ('cen02._S', 'cen54._S', 'cen15._S','alta_clinica._S', 
                                'triagem._S', 'intb6._S', 'intb0._S', 'cen70._S', 
                                'cen62._S', 'lab63._S')
    SELECTED_POSTGRES_TABLES = ('hp_rege_evolucao', 'hp_rege_ralta', 'hp_rege_receituario', 'hp_descricao_cirurgia',
                                'hp_rege_emerg', 'hp_prontuario_be')
    