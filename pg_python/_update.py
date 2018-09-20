import logging


def make_postgres_update_statement(table, kv_map, where_kv_map, clause, debug = True):
    _prefix = "UPDATE"
    clause = " " + clause + " "
    keys = ", ".join([k + "=%s" for k in list(kv_map.keys())])
    where_keys = " AND ".join([k + clause + "%s" for k in list(where_kv_map.keys())])
    value_proxy_array = ["%s"] * len(kv_map)
    value_proxy_string = ", ".join(value_proxy_array)
    statement = " ".join([_prefix, table, "SET", keys, "WHERE", where_keys])
    if debug:
        logging.info("Updating into Db: %s, %s" %(statement, list(kv_map.values()) + list(where_kv_map.values())))
    return statement, list(kv_map.values()) + list(where_kv_map.values())


def get_from_clause(query_values_dict_lst,columns_to_query_lst):
    """
    returns from clause that contains tuples of placeholders
    :param query_values_dict_lst:
    :param columns_to_query_lst:
    :return:
    """
    from_str = "from (values "
    length = len(columns_to_query_lst)+1
    placeholder_str = ["%s"] * length
    row_str = ",".join(placeholder_str)
    row_str = "(" + row_str + ")"
    num_of_dict = len(query_values_dict_lst)
    multi_row_str = [row_str]*num_of_dict
    multi_row_str = ",".join(multi_row_str)
    from_str = from_str +multi_row_str +")"
    return from_str


def get_as_clause(columns_to_query_lst):
    """
    get_as_clause will return all column names tuples.
    :param columns_to_query_lst: columns for where clause
    :return:
    """
    column_str = ""
    for col in columns_to_query_lst:
        column_str = column_str + col + ","
    column_str += "update"
    as_clause = "as c(" + column_str + ")"
    return  as_clause


def get_where_clause(columns_to_query_lst):
    """
    get_where_clause returns the where clause from the given query list.
    :param columns_to_query_lst: columns for where clause.
    :return:
    """
    where_str = "where "
    equals_str =[]
    for row in columns_to_query_lst:
        temp_str =  "c." + row + " = t." + row
        equals_str.append(temp_str)
    joined_str = " AND ".join(equals_str)
    where_str = where_str + joined_str
    return where_str


def get_values(column_to_query_lst, query_values_dict_lst ):
    """
    makes flat list for update values.
    :param column_to_query_lst:
    :param query_values_dict_lst:
    :return:
    """
    column_to_query_lst.append("update")
    values = []
    for dict_row in query_values_dict_lst:
        for col in column_to_query_lst:
            values.append(dict_row[col])
    return values

def make_postgres_update_multiple_statement(table,column_to_update,
                                            columns_to_query_lst,
                                            query_values_dict_lst,
                                            print_debug_log = True):
    """
    It makes query statement.
    :param table: table to update.
    :param column_to_update: column name that is to be updated.
    :param columns_to_query_lst: columns name that will we used for where clause.
    :param query_values_dict_lst: list of dictionary that contains values to update.
    :param print_debug_log:
    :return:
    """
    _prefix = "UPDATE"
    table_name = table + " as t"
    keys = column_to_update + " = c.update"
    from_clause = get_from_clause(query_values_dict_lst, columns_to_query_lst)
    as_clause = get_as_clause(columns_to_query_lst)
    where_clause = get_where_clause(columns_to_query_lst)
    statement = " ".join([_prefix, table_name, "SET", keys, from_clause, as_clause, where_clause])
    values = get_values(columns_to_query_lst, query_values_dict_lst)
    if print_debug_log == True:
       logging.info("Updating multiple rows into db %s"%(statement))
    return  statement ,values

