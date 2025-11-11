from db_analysis_dependencies import compare_pipeline as pipeline
from db_analysis_dependencies import form_structures
from db_analysis_dependencies import io_handler
from datetime import datetime

version='1.0'

def main():
    #start of the program
    io_handler.print_timestamp('starting')
    request = str('SELECT * FROM KATA_task_taskerrors')
    #gather data from database and present it as list of dictionaries
    io_handler.print_timestamp('forming dictionary lists')
    data, headers, selected_fields = form_structures.form_dictionary_lists_from_scratch(io_handler.db_request(request, db_addr=args.database_path),\
                                                                    column_passed=args.column_selected, filter=args.filter_column, rows_limit=args.gather_rows_limit)
    total_score = len(data)
    #checking the file-path if there are available cluster centers to start focusing data around with
    io_handler.print_timestamp('reading cluster centers from file')
    try:
        cluster_centers = form_structures.get_cluster_centers_from_file(args.cluster_centers_file_path)
    except Exception as err:
        print(err)
        cluster_centers = dict()
    #checking the file-path if there are regular expressions available for use during tokenization process
    token_filter = form_structures.set_tokenizer_regex(args.token_regex_file_path)
    #focusing data around available cluster centers loaded from file-path
    data_clusters, data_unsorted = form_structures.form_k_cluster(selected_fields, cluster_centers, similarity=args.similarity, tokenizer_limit=args.tokenizer_limit,\
                                                                    print_score=args.print_score)
    #comparing data within clusters, then unsorted one
    result = pipeline.jaccard_compare_pipeline(selected_fields, std_approx=args.similarity, data_clusters=data_clusters, data_unsorted=data_unsorted,\
                                     cluster_centers_file_path=args.cluster_centers_file_path, print_score=args.print_score, tokenizer_limit=args.tokenizer_limit,\
                                     token_regex_filter=token_filter, total_score=total_score)
    if args.print_score:
        io_handler.print_timestamp('rows processed in total: {}/{}'.format(pipeline.processed_score, total_score))        
    #writing result dictionary into json file-path
    io_handler.print_timestamp('writing into json file')
    io_handler.data_write_into_file(data, result, headers, crit=args.column_selected, link=args.link_field, out_addr=args.output_file_path, field_name_len=args.output_length)
    io_handler.print_timestamp('finishing')
    return

if __name__ == '__main__':
    prog = __file__.split('\\')[-1]
    args = io_handler.get_args(prog, version=version)
    main()