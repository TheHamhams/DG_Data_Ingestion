import pandas as pd
import logging
import yaml
import datetime
import re

def read_config(path):
    with open(path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)
            
def whitespace_remover(dataframe):
    for i in dataframe.columns:
        if dataframe[i].dtype == 'object':
            dataframe[i] = dataframe[i].map(str.strip)
       
            
def col_header_val(df, table_config):
    whitespace_remover(db)
    df.columns = df.columns.str.lower()
    expected_col = list(map(lambda x: x.lower(), table_config['columns']))
    expected_col.sort()
    df.columns = list(map(lambda x: x.lower(), list(df.columns)))
    df = df.reindex(sorted(df.columns), axis=1)
    
    if len(df.coluns) == lend(expected_col) and list(expected_col) == list(df.columns):
        print('Columns names and length validattion passed!')
        return 1
    else:
        print('Column names and length validation failed')
        mismatched_columns_file = list(ser(df.columns).difference(expected_col))
        print(f'The following file columns are not present in the YAML file: {mismatched_columns_file}')
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print(f'The following YAML columns are not in the uploaded file: {missing_YAML_file}')
        logging.info(f'df_columns: {df.columns}')
        logging.info(f'expected_columns: {expected_col}')
        return 0