import logging
import yaml
import re
from os.path import getsize
import gzip

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

def check_results(validation, df, name, delimiter):    
    if validation == 0:
        print('Validation failed')
    else:
        print('Validation passed')
        make_file(df, name, delimiter)
        
        
    
def make_file(df, outgoing , delimiter):
    outgoing_name = outgoing + '.txt'
    df.to_csv(outgoing_name, header=None, index=None, sep=delimiter, mode='a')
    f_in = open(outgoing_name , 'rb')
    f_out = gzip.open(f'{outgoing}.txt.gz', 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

    
def df_info(df, name):
    path = f'{name}.csv'
    size = getsize(path) / 1024**2
    print(f"""
    Number of columns: {len(df.columns)}
    Number of rows: {len(df)}
    File size: {round(size, 2)}MB 
    """)   