from crewai import Agent, Task, Crew
from weighted_levenshtein import lev
import numpy as np
from unidecode import unidecode
import pandas as pd
import re
import json
from vertexai.language_models import TextGenerationModel
from pipelines.utils.credential_injector import authenticated_task as task

@task
def create_allergie_list(dataframe_allergies_vitai:pd.DataFrame):
    '''
    Create a list of allergies from a dataframe
    Args:
        dataframe (pd.DataFrame): Dataframe with the allergies
    '''
    def clean_allergies_field(allergies_field):
        allergies_field_clean = re.sub('INTOLERANCIA A{0,1}|((REFERE ){0,1}ALERGIA A{0,1})|(PACIENTE ){0,1}AL[É|E]RGIC[A|O] AO{0,1}','',allergies_field)
        allergies_field_clean = re.sub('.*N[Ã|A]O [(SABE)|(RECORDA)|(LEMBRA))].*','',allergies_field_clean)
        allergies_field_clean = re.sub('.*COMENTARIO.*','',allergies_field_clean)
        allergies_field_clean = re.sub('.*LISTA.*','',allergies_field_clean)
        allergies_field_clean = re.sub(' E |\/|\n',',',allergies_field_clean)
        allergies_field_clean = re.sub('(\\\)',',',allergies_field_clean)
        return allergies_field_clean
    
    dataframe_allergies_vitai['text_input_clean'] = dataframe_allergies_vitai['agg_alergia'].apply(lambda x: clean_allergies_field(x))
    alergias = dataframe_allergies_vitai['text_input_clean'].values
    alergias_join = ','.join(alergias)
    alergias_lista = alergias_join.split(',')
    alergias_lista = list(set([alergia.strip() for alergia in alergias_lista]))
    return alergias_lista

@task
def get_similar_allergie_levenshtein(allergies_dataset_reference:list, allergie_list:list,threshold:float):
    '''
    Get similar allergie using levenshtein distance
    Args:
        allergies_dataset_reference (list): List of allergies to be used as reference
        allergie_list (list): List of allergie to be standardized
        threshold (float): Threshold for the levenshtein distance
    '''
    # Defining weighted-levenshtein function
    def dist(c1, c2,key_positions):
        pos1 = key_positions[c1]
        pos2 = key_positions[c2]
        return abs(pos1[0] - pos2[0]) + abs(pos1[1] - pos2[1])
    key_positions = {"Q":(0,0), "W":(0,1), "E":(0,2), "R":(0,3), "T":(0,4), "Y":(0,5), "U":(0,6), "I":(0,7), "O":(0,8), "P":(0,9),
                     "A":(1,0), "S":(1,1), "D":(1,2), "F":(1,3), "G":(1,4), "H":(1,5), "J":(1,6), "K":(1,7), "L":(1,8),
                     "Z":(2,0), "X":(2,1) ,"C":(2,2), "V":(2,3), "B":(2,4), "N":(2,5), "M":(2,6)}
    
    substitute_costs = np.ones((128, 128), dtype=np.float64)
    for key_1 in key_positions.keys():
        for key_2 in key_positions.keys():
            if dist(key_1,key_2,key_positions) == 1:
                substitute_costs[ord(key_1), ord(key_2)] = 0.5 

    # Calculating weighted-levenshtein similarity
    standardized_list=[]
    not_standardized_list=[]
    for allergie in allergie_list:
        candidates=[]
        similaritys=[]
        for ref in allergies_dataset_reference['descricao_clean'].values:
            lev_result = lev(ref, allergie, substitute_costs=substitute_costs)
            max_length = max(len(ref),len(allergie))
            lev_weighted_similarity = 1-(lev_result/max_length)

            if lev_weighted_similarity > threshold:
                candidates.append(ref)
                similaritys.append(lev_weighted_similarity)
        result = pd.DataFrame({'elemento_escolhido':candidates,'similaridade':similaritys})
        result.sort_values(by='similaridade',ascending=False,inplace=True)
        if result.head(1).empty:
            not_standardized_list.append(allergie)
        else:
            standardized_list.append(result.head(1).to_dict(orient='records'))
    return standardized_list, not_standardized_list

@task
def get_similar_allergie_gemini(allergies_list:list,key:str,batch_size:int=40):
    '''
    Get similar allergie using gemini agent
    Args:
        allergies_list (list): List of allergies to be standardized
        key (str): Gemini API key
        batch_size (int): Batch size for the gemini agent
    '''
    ### Defining gemini crew
    llm = LLM(
        model="gemini/gemini-pro", temperature=0.9,
        api_key=key,
    )

    buscador = Agent(
        llm=llm,
        role="Especialista de alergias",
        goal="Compreender os elementos causadores de alergia a um paciente, fazendo correções de grafia  nos inputs se necessário.",
        backstory="Você trabalha em uma unidade de saúde onde o preenchimento do "
                "campo de alergias no prontuario de pacientes tem diversos erros "
                "de digitação e mal preenchimento. Sua função é compreender e tratar "
                "as alergias inputadas descartando elementos que não se referirem a alergias."
                "Sempre que você nao estiver certo que um elemento se refere a uma alergia se pergunte se "
                "o elemento pode gerar uma alergia em uma pessoa, sabendo que as alergias podem se tratar de medicamentos, alimentos ou substâncias.",
        verbose=True
    )

    limpeza = Task(
        description=(
            "1. Observe o input {alergias} e compreenda os causadores de alergias dentro do input. "
            "O input pode ser diretamente o termo que causa alergia ou uma frase contendo um termo que causa alergia. Os causadores de alergia são " 
            "alimentos, medicamentos ou substâncias que podem estar com grafia incorreta. \n"
            "2. Faça uma tratamento no input corrigindo a grafia dos elementos. Elementos que nao causam alergias são definidos como tudo "
            "o que não se trata de medicamentos, alimentos ou substâncias e devem ser retornados com output sendo uma string vazia "". \n"
            "3. O que você não tenha certeza do que se trata deve ser retornados com output sendo uma string vazia "" \n"
            "3. Retorne a lista de alergias \n"
        ),
        expected_output='Lista limpa cujos elementos são as alergias com grafia correta e que correspondem aos da lista input. No formato [{{"input":"alergia 1","output":"alergia limpa 1","motivo":"motivo do preenchimento do output"}},{{"input":"alergia 2","output":"alergia limpa 2","motivo":"motivo do preenchimento do output sem vírgulas"}}]',
        agent=buscador,
    )
    crew = Crew(
        agents=[buscador],
        tasks=[limpeza],
        verbose=True,
        memory=False
    )
    ### Executing gemini standardization
    result_list = []
    for i in range(0, len(allergies_list), batch_size):
        aux_list = allergies_list[i:i + batch_size]
        inputs_array = {'alergias': str(aux_list)}
        result = crew.kickoff(inputs=inputs_array)
        resultado = result.raw.replace("'",'"')
        resultado = resultado.replace('None','""')
        resultado = resultado.replace('\n','')
        result_list.extend(json.loads(resultado))
    return result_list

@task 
def validade_medlm(gemini_result:list,levenshtein_result:list):
    '''
    Validate gemini result using medlm model
    Args:
        gemini_result (list): List of gemini result
        levenshtein_result (list): List of levenshtein result
    '''
    model_instance = TextGenerationModel.from_pretrained("medlm-large")
    clean_mapping = []
    for i,o in gemini_result.items():
        if i == o:
            clean_mapping.append({"input":i,"output":o,"output_medlm":{'flag':1,'motivo':'Mesmo elemento'}})
        elif o != '':
            response = model_instance.predict(
                f'''
            Você receberá duas entradas que devem se tratar de causadores de alergia, ou seja, medicamento, classe de medicamentos, substâncias ou alimentos. 
            Avalie se o input {i}, que possivelmente está escrito de forma errada, e o output {o} se referem ao mesmo causador de alergia. 
            Caso um seja uma generalização do outro considere que são a mesma coisa.
            Ex: AINES e Cetoprofeno
            Retorne flag 1 caso verdadeiro e 0 caso falso. A saída deverá ter a explicação da resposta em um JSON válido no formato:
            {{"flag": Flag associada, "motivo": Explicação sucinta e sem vírgulas do valor associado a flag}}''',
            )
            json_str = re.search('{.*\n*.*}', response.text, re.IGNORECASE).group()
            json_str = json_str.replace('\n',' ')
            response = json.loads(json_str)
        
            clean_mapping.append({"input":i,"output":o,"output_medlm":response})
        elif o == '':
            response = model_instance.predict(
                f'''
            Você receberá um input que possivelmente está escrito de forma errada.
            Avalie se o input {i} se refere a um causador de alergia. Pode ser um alimento, medicamento ou substância.
            Ex: 'AINES', 'Alergia a Cetoprofeno'
            Retorne flag 0 caso verdadeiro e 1 caso falso. A saída deverá ter a explicação da resposta em um JSON válido no formato:
            {{"flag": Flag associada, "motivo": Explicação sucinta e sem vírgulas do valor associado a flag}}''',
            )
            json_str = re.search('{.*\n*.*}', response.text, re.IGNORECASE).group()
            json_str = json_str.replace('\n',' ')
            response = json.loads(json_str)
            
            clean_mapping.append({"input":i,"output":o,"output_medlm":response})
    clean_mapping_df = pd.json_normalize(clean_mapping)
    clean_mapping_df.loc[clean_mapping_df['output_medlm.flag']==0,'output']= clean_mapping_df.loc[clean_mapping_df['output_medlm.flag']==0,'input']
    clean_mapping_df['output'] = clean_mapping_df['output'].str.replace('/',',')
    clean_mapping_df['output'] = clean_mapping_df['output'].str.replace('\n',',')
    levenshtein_result.extend(clean_mapping_df.to_dict(orient='records'))

    return levenshtein_result

