import prettytable
import time
from datetime import datetime
import sqlite3
import json
from db_analysis_dependencies import form_structures
import argparse

def get_args(name='undefined_program_path', version='undefined'):
    parser = argparse.ArgumentParser(prog=name,
                    description='Agregates data around fields of selected column in database. With use of jaccard method and k-cluster optimization and sqlite3 support.\n\
                    Version {}'.format(version))
    
    parser.add_argument('-v', '--version', action='version', version='%(prog)s {}'.format(version))
    parser.add_argument('-s', '--similarity', \
                help='Set the minimum similarity coefficient. This affects the range of aggregation: \
                the lower the value sets – the longer the range gets.\nThe value must be float number type.', type=float, default=0.7)
    parser.add_argument('-c', '--column_selected', \
                help='Set the column name to process.', type=str)
    parser.add_argument('-C', '--cluster-centers-file-path', \
                help='Set the cluster centers file path. Cluster centers can be used for initial data aggregation around forming clusters.\nThe value must be string.',\
                type=str, default='cluster_centers.json')
    parser.add_argument('-r', '--token-regex-file-path', \
                help='Set the regular expressions file path. Regular expressions can be used for filtering data before aggregation.\nThe value must be string.',\
                type=str, nargs='?', const='token_regex.json', default=None)
    parser.add_argument('-t', '--tokenizer-limit', help='Set token sequence length limit. This affects the tokenizer in means of text processing length threshold.\
                The value must be integer.', type=int, default=20)
    parser.add_argument('-g', '--gather-rows-limit', \
                help='Set gathering rows number limit. This affects the amount of rows being taken from source for processing. The value must be integer.',\
                type=int, default=None)
    parser.add_argument('-f', '--filter-column', help='Include field value filtering. This filters sufficient data cutting custom delimiters off.', action='store_true',\
                default=False)
    parser.add_argument('-p', '--print-score', help='Include score printing in the console output.', action='store_true', default=False)
    parser.add_argument('-o', '--output-file-path', help='Set output file path. Format depends on operating system. The value must be string.',\
                         type=str, default='db_analysis_output.json')
    parser.add_argument('-d', '--database-path', help='Set source database path. The value must be string.', type=str, default='db.sqlite3')
    parser.add_argument('-l', '--link-field', help='Set link field that is used for result presentation.', type=str)
    parser.add_argument('-L', '--output-length', help='Set lenght of output text.', type=int, default=128)
    args = parser.parse_args()
    return args

def print_timestamp(message='ping'):
    timer = datetime.fromtimestamp(time.time())
    print('{}\ntimestamp: {}'.format(message, timer))

#make request to target database getting raw data from it
def db_request(request:str, db_addr:str='db.sqlite3'):
    connect = sqlite3.connect(db_addr)
    cursor = connect.cursor()
    raw_data = cursor.execute(request)
    return raw_data

#in case of console output
def data_print(prepared_data, headers:list, width:int=25, print_limit=None):
    table = prettytable.PrettyTable()
    #setting up the headers
    for column in headers:
        table.add_column(column, [])
    ctr = int(0)
    #adding rows according the headers (limited amount)
    for rows in prepared_data:
        if print_limit:
            if ctr > print_limit:
                break
        table.add_row([rows.get(column, "") for column in headers])
        ctr += 1
    #setting up the width of column
    table.max_width=width
    return table

def data_write_into_file(data, res, fields, crit, link=None, out_addr='out.json', field_name_len=128):
        with open(out_addr, 'w') as file_w:
            json.dump(form_structures.form_json_from_data(data, res, fields, crit=crit, spec_field=link, field_name_len=field_name_len), file_w, indent=4, ensure_ascii=True)