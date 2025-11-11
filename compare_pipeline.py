import hashlib
import json
import struct
import re
from db_analysis_dependencies.io_handler import print_timestamp

processed_score=0
#comparison
def hash_sha1(data:str):
    return struct.unpack('<I', hashlib.sha1(data).digest()[:4])[0]

def tokenize_data(raw_data, regex_filter=None, tokenizer_limit:int=None):
    if regex_filter:
        data_tokenized = ["".join(x) for x in re.findall(r'{}'.format(regex_filter), raw_data)]
    else:
        data_tokenized = raw_data.split(' ')
    data_hash_list = list()
    if len(data_tokenized) > tokenizer_limit:
        data_tokenized = data_tokenized[:tokenizer_limit]
    for token in data_tokenized:
        data_hash_list.append(hash_sha1(token.encode('utf-8')))
    return data_hash_list

def jaccard_comp(data_hash_list_1, data_hash_list_2):
    data_set_1 = set(data_hash_list_1)
    data_set_2 = set(data_hash_list_2)
    try:
        jaccard = float(len(data_set_1.intersection(data_set_2))) / float(len(data_set_1.union(data_set_2)))
    except ZeroDivisionError:
        if data_set_1:
            jaccard = 0
        else:
            jaccard = 1
    return jaccard

def compare_data(data, indexes:list=None, approx=float(0.4), tokenizer_limit:int=None, token_regex_filter:list=None, print_score:bool=False, total_score:int=None, cluster_name='unsorted'):
    global processed_score
    if not indexes:
        indexes = data
    nodes_comparison = dict()
    nodes_comparison.update({indexes[0] : tuple([tokenize_data(data[indexes[0]], regex_filter=token_regex_filter, tokenizer_limit=tokenizer_limit), [indexes[0]]])})
    check_unique = bool(True)
    try:
        for i in indexes[1:]:
            for j in nodes_comparison:
                data_tokenized = tokenize_data(data[i], regex_filter=token_regex_filter, tokenizer_limit=tokenizer_limit)
                jacc = jaccard_comp(data_tokenized, nodes_comparison[j][0])
                if (jacc >= approx):
                    nodes_comparison[j][1].append(i)
                    check_unique = False
                    continue
            if check_unique:
                nodes_comparison.update({i : tuple([data_tokenized, [i]])})
            check_unique = True
            if print_score:
                processed_score += 1
                print('{}:rows processed: {}/{}'.format(cluster_name, processed_score, total_score), end='\r')
    except KeyboardInterrupt:
        pass
    return nodes_comparison

def update_cluster_centers(result_sorted:dict, target_fields:dict, cluster_centers_file_path:str=None):
    print_timestamp('updating cluster centers based on sorted data')
    if not cluster_centers_file_path:
        cluster_centers_file_path = 'cluster_centers.json'
    for index in result_sorted:
        if len(result_sorted[index][1]) > 9:
            content = dict()
            try:
                with open(cluster_centers_file_path, 'r') as file_r:
                    content = json.load(file_r)
            except Exception:
                content = dict()
            content.update({len(content) : target_fields[index]})
            with open(cluster_centers_file_path, 'w') as file_w:
                json.dump(content, file_w, indent=4, ensure_ascii=True)
    return

def jaccard_compare_pipeline(target_fields:dict, std_approx=float(0.4), data_clusters:dict=None, data_unsorted:dict=None, cluster_centers_file_path:str=None,\
                            tokenizer_limit:int=None, token_regex_filter:list=None, print_score:bool=False, total_score:int=None):
    res = dict()
    for center_idx in data_clusters:
        cluster_res = dict()
        if data_clusters[center_idx]:
            #cluster_res = compare_data(target_fields, data_clusters[center_idx], approx=std_approx, print_score=print_score, tokenizer_limit=tokenizer_limit,\
            #    token_regex_filter=token_regex_filter, total_score=total_score, cluster_name=center_idx)
            res.update({data_clusters[center_idx][0]: tuple(['tokenized', data_clusters[center_idx]])})
            #print(list(cluster_res.keys())[0], data_clusters[center_idx] == cluster_res.get(list(cluster_res.keys())[0])[1])
            #res.update(cluster_res)
    #exit()
    if data_unsorted:
        prev_prepr_score = processed_score
        print_timestamp('comparing data unsorted')
        sorted_res = compare_data(target_fields, data_unsorted, approx=std_approx, print_score=print_score, tokenizer_limit=tokenizer_limit, token_regex_filter=token_regex_filter,\
                                 total_score=total_score)
        if print_score:
            print('unsorted:rows processed: {}/{} (+{})'.format(processed_score, total_score, processed_score - prev_prepr_score))
        res.update(sorted_res)
        update_cluster_centers(sorted_res, target_fields, cluster_centers_file_path=cluster_centers_file_path)
    return res

