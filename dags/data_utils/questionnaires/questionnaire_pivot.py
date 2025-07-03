import pandas as pd
from ..postgres_helper.postgres_helper import dump_data_to_postgres, get_postgres_connection

# do NOT add ps_belge 1 and 92 
questionnaires_ids_dict = {
    "marseille": [456,559,560,614],
    "lyon": [611,653],
}

def retrieve_form_answers(questionnaire_id, engine):
    query = f"""
                SELECT 
                    session_token,
                    question_type,
                    question_title,
                    answer,
                    decidim_questionnaire_id,
                    position
                FROM prod.forms_answers
                WHERE question_type = 'single_option'
                AND decidim_questionnaire_id = {questionnaire_id}
            """
    connection = engine.connect()
    form_answers = pd.read_sql(query, connection)
    connection.close()

    if form_answers.empty:
        raise AssertionError(f"No questionnaire found with id {questionnaire_id}")
    
    return form_answers

def concat_multiple_answers(form_answers):
    "Concatenates multiple answers per question into a single answer"

    df = form_answers.copy()

    df['answer'] = df.\
        groupby(['session_token', 'position'])['answer'].\
        transform(lambda x : ' '.join(x))
    df = df.drop_duplicates(subset=['session_token', 'position', 'answer'])

    return df

def pivot_filters(form_answers):
    "Pivots a form_answers dataframe in order to generate a table to use in Metabase as questionnaire filters"
    
    form_answers['filter_name'] = form_answers['position'].astype(str) + '. ' + form_answers['question_title']
    form_answers['filter_name'] = form_answers['filter_name'].str[:40]
    form_filters = form_answers.pivot(index='session_token', columns='filter_name', values='answer')
    form_filters.columns=form_filters.columns.str.lower().str.replace(' ','_').str.replace("'","_")
    form_filters=form_filters.reset_index()

    return form_filters

def fetch_and_dump_answers_data(db_name, questionnaire_id):
    engine = get_postgres_connection("main_db_cluster_name", db_name)
    connection = engine.connect()

    form_answers = retrieve_form_answers(questionnaire_id, engine)

    form_unique_answers = concat_multiple_answers(form_answers)

    form_filters = pivot_filters(form_unique_answers)

    table_name = f'questionnaire_{questionnaire_id}_filters'

    dump_data_to_postgres(connection, form_filters, table_name, schema='forms', if_exists='replace')
    connection.close()

def create_questionnaire_filters(database_name, client_name):

    questionnaires_ids = questionnaires_ids_dict.get(client_name, [])
    
    for questionnaire_id in questionnaires_ids:
        fetch_and_dump_answers_data(database_name, questionnaire_id)

