import json
import re
from db_analysis_dependencies import compare_pipeline
from db_analysis_dependencies import io_handler

def filter_field(field):
    res = False
    if field == '-':
        res = True
    return res

def form_dictionary_lists_from_scratch(raw_data, column_passed:str=None, clear:bool=True, filter:bool=False, rows_limit=None):
    headers = list()
    #fetch column names
    for elem in raw_data.description:
        headers.append(elem[0])
    #throw raw data elements into dictionaries and form list of them
    column_pos = int(0)
    max_num_of_columns = len(headers)
    prepared_data = list()
    field_counter = int(0)
    selected_fields = dict()
    for rows in raw_data:
        row = dict()
        for elem in rows:
            row.update({headers[column_pos] : elem})
            column_pos += 1
        #... wile excluding rows with empty error output
        if filter:
            if row.get(column_passed):
                res = filter_field(row[column_passed])
                row[column_passed] = re.sub(r'\n*\#+\n*', '\n', row[column_passed])
                if res:
                    column_pos = 0
                    continue
            #row['success_output'] = re.sub(r'\n*\#+\n*', '\n', row['success_output'])
        prepared_data.append(row)
        selected_fields.update({field_counter : row[column_passed]})
        field_counter += 1
        #DB ROWS LIMIT
        if rows_limit:
            if field_counter > rows_limit:
                break 
        column_pos = 0
    return prepared_data, headers, selected_fields

def form_k_cluster(selected_data, cluster_centers:dict, similarity = float(0.9), tokenizer_limit:int=None, print_score:bool=False):
    io_handler.print_timestamp('forming clusters based on centers')
    data_distributed = dict()
    data_unsorted = list()
    total_score = len(selected_data)
    whitespace = 0
    for center in cluster_centers:
        data_distributed.update({center : list()})
        if whitespace < len(str(center)):
            whitespace = len(str(center))
    is_distributed = False
    try:
        for index in selected_data:
            for center in cluster_centers:
                if compare_pipeline.jaccard_comp(compare_pipeline.tokenize_data(selected_data[index], tokenizer_limit=tokenizer_limit),\
                compare_pipeline.tokenize_data(cluster_centers[center], tokenizer_limit=tokenizer_limit)) >= similarity:
                    data_distributed[center].append(index)
                    is_distributed = True
                    compare_pipeline.processed_score += 1
                    if print_score:
                        print('{}{}:rows processed: {}/{}'.format(center, ' ' * (whitespace - len(str(center))), compare_pipeline.processed_score, total_score), end='\r')
                    break
            if not is_distributed:
                    data_unsorted.append(index)
            is_distributed = False
    except KeyboardInterrupt:
        pass
    if print_score and compare_pipeline.processed_score > 0:
        print()
    return data_distributed, data_unsorted

def form_json_from_data(data_list, selection, \
                        fields=None, \
                        crit='error_output', spec_field='do_name', field_name_len=128):
    prep_dict = dict()
    add_fields = fields
    #getting rid of fields those are above the description
    for rem in [crit, spec_field]:
        add_fields.remove(rem)
    #merging simillar data under criteria field
    for elem in selection:
        head = dict()
        link = dict()
        for index in selection[elem][1]:
            desc = dict()
            desc.update(data_list[index])
            #adding link just in means of presentation
            if desc[spec_field] in link:
                for field in add_fields:
                    link[desc[spec_field]][field].append(desc[field])
            else:
                for field in add_fields:
                    desc.update({field : [desc[field]]})
                link.update({desc[spec_field] : desc})
        #shorten field value lenght
        short_field = lambda name : (u'{}'.format(name[:field_name_len]) + '..') if len(name) > field_name_len else name
        head.update({short_field(data_list[elem][crit]): link})
        #adding merged data to collecting dictionary        
        prep_dict.update(head)
    return prep_dict

def get_cluster_centers_from_file(cluster_centers_path:str):
    cluster_centers = dict()
    with open(cluster_centers_path, 'r') as file_r:
        cluster_centers = json.load(file_r)
    return cluster_centers

def set_tokenizer_regex(regex_list_path=None):
    regex_list = dict()
    regex_str = str()
    if regex_list_path:
        with open (regex_list_path, 'r') as file_r:
            regex_list= json.load(file_r)
        if regex_list:
            for exp in regex_list:
                regex_str += u'(?P<{}>{})|'.format(exp, regex_list[exp])
            regex_str=regex_str[:-1]
    return regex_str
    