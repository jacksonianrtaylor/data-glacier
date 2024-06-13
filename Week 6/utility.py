import logging
import os
import subprocess
import yaml
import pandas as pd
import datetime 
import gc
import re
import gzip
from time import time



################
# File Reading #
################

def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)


def replacer(string, char):
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string) 
    return string

def col_header_val(df,table_config):
    '''
    replace whitespaces in the column
    and standardized column names
    '''
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('[^\w]','_',regex=True)
    df.columns = list(map(lambda x: x.strip('_'), list(df.columns)))
    df.columns = list(map(lambda x: replacer(x,'_'), list(df.columns)))
    expected_col = list(map(lambda x: x.lower(),  table_config['columns']))
    expected_col.sort()
    df.columns =list(map(lambda x: x.lower(), list(df.columns)))
    df = df.reindex(sorted(df.columns), axis=1)
    if len(df.columns) == len(expected_col) and list(expected_col)  == list(df.columns):
        print("column name and column length validation passed")
        return 1
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        print("Following File columns are not in the YAML file",mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print("Following YAML columns are not in the file uploaded",missing_YAML_file)
        logging.info(f'df columns: {df.columns}')
        logging.info(f'expected columns: {expected_col}')
        return 0


def execution_time(func,type, *args, **kwargs):
    """time the execution of a function"""
    start = time()
    func(*args, **kwargs)
    print('Time to read with '+type+':', time() - start, 'seconds')


def remove_file(table_config):
    """remove file corresponding to table_config"""
    file_type = table_config['file_type']
    source_file = "./" + table_config['file_name'] + f'.{file_type}'
    os.remove(source_file)


def write_file(df, table_config, file_name):
    """write new .txt.gz file with df data using file_name and table_config"""
    with gzip.open(file_name, 'wt') as file:
        file.write(table_config["outbound_delimiter"].join(table_config["columns"]))
        for row in df.values:
            file.write('\n'+ table_config["outbound_delimiter"].join(map(str, row)))


def stats(table_config, file_name):
    """return stats of the file specified by file_name"""
    with gzip.open(file_name, 'rt') as file:
        file_size = os.path.getsize(file.name)
        nof_columns = len(file.readline().strip().split(table_config["outbound_delimiter"]))
        nof_entries = len(file.readlines())
        print(file_name, "stats:")
        print("Number of rows excluding header:", nof_entries)
        print("Number of columns:", nof_columns)
        print("File size:", file_size, "bytes")
