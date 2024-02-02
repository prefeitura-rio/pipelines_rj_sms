'''
Utils for standartize data
'''
import re
import pandas as pd


def gender_validation(gender: str) -> str:
    """
    Format gender to API accepted values
    Args:
        gender (str): Raw gender information
    Returns:
        gender_std (str): Normalized gender information
    """
    if gender != None:
        gender = gender.lower()
    else:
        return None

    if gender == 'F':
        gender_std = 'female'
    elif gender == 'M':
        gender_std = 'male'
    else:
        gender_std = 'unknown'

    return gender_std


def nationality_validation(nationality: str) -> str:
    """
    Format gender to values accepted by API
    Args:
        nationality (str): Raw nationality information
    Returns:
        nationality_std (str): Normalized nationality information
    """
    if nationality != None:
        nationality = nationality.lower()
    else:
        return None

    if bool(re.search("brasileir[a|o]", nationality)):
        nationality_std = 'B'
    elif bool(re.search("naturalizad[a|o]", nationality)):
        nationality_std = 'N'
    elif bool(re.search("estrangeir[a|o]", nationality)):
        nationality_std = 'E'
    else:
        nationality_std = None

    return nationality_std


def state_cod_validation(state: str) -> str:
    """
    Format state to codes accepted by API
    Args:
       state (str): Raw state information
    Returns:
        state_std (str): Normalized state information
    """

    df_states = pd.read_csv('pipelines/prontuarios/std/estados.csv')
    df_states['SIGLA'] = df_states['SIGLA'].str.lower()
    df_states['SIGLA'] = df_states['SIGLA'].str.replace(' ', '')
    if state != None:
        state = state.lower()
        state_std = str(df_states.loc[df_states['SIGLA'] == state, 'COD'].values[0])
    else:
        return None

    return state_std


def city_cod_validation(city: str) -> str:
    """
    Format city to codes accepted by API
    Args:
       city (str): Raw city information
    Returns:
        city_std (str): Normalized city information
    """
    df_city = pd.read_csv('pipelines/prontuarios/std/municipios.csv')
    df_city['NOME'] = df_city['NOME'].str.lower()
    if city != None:
        city = city.lower()
        city_std = str(df_city.loc[df_city['NOME'] == city, 'COD'].values[0])
    else:
        return None

    return city_std
