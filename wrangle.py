from env import get_db_url
import pandas as pd
import numpy as np







def get_data():
    try:
        df = pd.read_csv('logs.csv')
        return df
    except FileNotFoundError:
        url = get_db_url('curriculum_logs')
        query = """
        select id, name, start_date, end_date, program_id, user_id, path, ip,
        date, time from cohorts right join logs on cohorts.id = logs.cohort_id
        """
        df = pd.read_sql(query, url)
        df.to_csv('logs.csv', index=False)
        return df
        
        
def get_prepped_data():
    df = get_data()
    df['date_time'] = df.date + ' ' + df.time
    df.date_time = pd.to_datetime(df.date_time)
    df.set_index(df.date_time, inplace=True)
    df.drop(columns=['time', 'date_time'])
    df.program_id = np.where((df.program_id == 1) | (df.program_id == 2), 1, df.program_id)
    df.loc[df.program_id == 3, 'program_id'] = 2
    df.loc[df['name'] == 'Staff', 'program_id'] = 3
    conditions = [
    df['program_id'] == 1,
    df['program_id'] == 2,
    df['program_id'] == 3
    ]
    choices = [
        'Web_dev',
        'Data Science',
        'Staff'
    ]
    df['program'] = np.select(conditions, choices)
    df.rename(columns={'id': 'cohort_id'}, inplace=True)
    df.fillna(0, inplace=True)
    return df
        
