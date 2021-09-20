import pandas as pd
import logging
import yaml
import datetime
import re
import os
import subprocess
import gc

def read_config(path):
    with open(path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)
            
def replacer(string, char):
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string) 
    return string

            
def col_header_val(df, table_config):
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('[^\w]','_',regex=True)
    df.columns = list(map(lambda x: x.strip('_'), list(df.columns)))
    df.columns = list(map(lambda x: replacer(x,'_'), list(df.columns)))
    expected_col = list(map(lambda x: x.lower(),  table_config['columns']))
    expected_col.sort()
    df.columns =list(map(lambda x: x.lower(), list(df.columns)))
    df = df.reindex(sorted(df.columns), axis=1)
    
    if len(df.columns) == len(expected_col) and list(expected_col) == list(df.columns):
        print('Columns names and length validattion passed!')
        return 1
    else:
        print('Column names and length validation failed')
        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        print(f'The following file columns are not present in the YAML file: {mismatched_columns_file}')
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print(f'The following YAML columns are not in the uploaded file: {missing_YAML_file}')
        logging.info(f'df_columns: {df.columns}')
        logging.info(f'expected_columns: {expected_col}')
        return 0