import logging


def make_postgres_read_statement(table, kv_map, keys_to_get, limit, order_by,
                                 order_type, debug, clause, group_by, join_clause):
    _prefix = "SELECT"
    _join_by = " " + join_clause + " "
    _table_string = " ".join(["FROM", table])
    null_keys = []

    for key in kv_map.keys():
        kval = kv_map.get(key)
        try:
            if str(kval).lower() == 'null':
                null_keys.append(key)
        except Exception as e:
            continue

    for key in null_keys:
        if key in kv_map:
            kv_map.pop(key, None)

    _key_string = _join_by.join([k + clause + "%s" for k in list(kv_map.keys())])
    _null_key_string = ""
    if len(null_keys) > 0:
        _null_key_string = _join_by.join([k + " is null " for k in null_keys])
    if len(_null_key_string) > 2:
        if len(_key_string) > 2:
            _key_string = _key_string + " and " + _null_key_string
        else:
            _key_string = _key_string + " " + _null_key_string

    values = list(kv_map.values())

    if clause.strip().lower() == "in":
        values = []
        _key_string = _join_by.join([k + clause + "(" + ",".join("'" + x + "'" for x in kv_map[k]) + ")" for k in list(kv_map.keys())])

    statement = " ".join([_prefix, ", ".join(sorted(keys_to_get)), _table_string])
    if len(list(kv_map.keys())) > 0 or len(null_keys) > 0:
      statement = " ".join([_prefix, ", ".join(sorted(keys_to_get)), _table_string, "WHERE", _key_string])
    if group_by is not None:
        statement += " GROUP BY " + group_by
    if order_by is not None:
        statement += " ORDER BY " + order_by + " " + order_type
    if limit is not None:
        statement += " LIMIT " + str(limit)
    if debug:
        logging.info("Reading From Db: %s, %s" %(statement, list(kv_map.values())))
    return statement, values


def prepare_values(all_values, keys_to_get, cols_keep_raw_type=[]):
    ret_val = []
    if all_values is None:
        return None
    k = sorted(keys_to_get)
    for row in all_values:
        row_kv = {}
        if len(row) == len(keys_to_get):
            for idx in range(0, len(row)):
                col_name = k[idx]
                if col_name in cols_keep_raw_type:
                    row_kv[k[idx]] = row[idx]
                else:
                    row_kv[k[idx]] = str(row[idx])
        else:
            logging.error("Number of keys to be fetched are not correct")
            continue
        ret_val.append(row_kv)
    return ret_val

