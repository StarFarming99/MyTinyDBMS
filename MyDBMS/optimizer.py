from prettytable import PrettyTable


# def print_table(res, type=None, limit=None):
#     """
#     Print the select relations
#     :param res: THis is result json like
#     res = {'COL1': ['No', 'YES', 'YES', 'YES', 'No', 'YES', 'YES', 'YES', 'YES'],
#        'COL2': [1.0, 7.0, 6.0, 4.0, 9.0, 11.0, 15.0, 18.0, 19.0]
#        }
#     :return:
#     """
#     if type:
#         for col, value in res.items():
#             if type[col][0] == 'int':
#                 for i in range(len(value)):
#                     value[i] = int(value[i])
#             elif type[col][0] == 'float':
#                 for i in range(len(value)):
#                     value[i] = float(value[i])
#             res[col] = value
#
#     tb = PrettyTable()
#     cols = list(res.keys())
#     for col in cols:
#         if limit:
#             if limit < len(res[col]):
#                 tb.add_column(col, res[col][0:limit])
#         elif len(res[col]) > 10000:
#             tb.add_column(col, res)
#         else:
#             tb.add_column(col, res[col])
#     print(tb)
#
# from prettytable import PrettyTable

def print_table(res, type=None, limit=None):
    if type:
        for col, value in res.items():
            if type[col][0] == 'int':
                for i in range(len(value)):
                    value[i] = int(value[i])
            elif type[col][0] == 'float':
                for i in range(len(value)):
                    value[i] = float(value[i])
            res[col] = value

    tb = PrettyTable()
    cols = list(res.keys())

    for col in cols:
        data = res[col]
        if limit:
            if limit < len(res[col]):
                data = data[:limit]
        elif len(res[col]) > 10000:
            data = data[:100] + ["..."] + data[-100:]
        tb.add_column(col, data)

    print(tb)

def is_return_false(condition, statistics):
    comp, value = condition['cond']['operation'], condition['cond']['value']
    if comp == '=':
        eval1 = eval(f"{statistics[condition['field']]['MAX']} < {value}")
        eval2 = eval(f"{statistics[condition['field']]['MIN']} > {value}")
        if eval1 or eval2:
            return True
    elif comp == '>=' and eval(f"{statistics[condition['field']]['MAX']} < {value}"):
        return True
    elif comp == '<=' and eval(f"{statistics[condition['field']]['MIN']} > {value}"):
        return True
    elif comp == '>' and eval(f"{statistics[condition['field']]['MAX']} <= {value}"):
        return True
    elif comp == '<' and eval(f"{statistics[condition['field']]['MIN']} >= {value}"):
        return True
    else:
        return False


def is_return_true(condition, statistics):
    comp, value = condition['cond']['operation'], condition['cond']['value']
    if comp == '>=' and eval(f"{statistics[condition['field']]['MIN']} >= {value}"):
        return True
    elif comp == '<=' and eval(f"{statistics[condition['field']]['MAX']} <= {value}"):
        return True
    elif comp == '>' and eval(f"{statistics[condition['field']]['MIN']} > {value}"):
        return True
    elif comp == '<' and eval(f"{statistics[condition['field']]['MAX']} < {value}"):
        return True
    else:
        return False


def zipping_condition(conditions, condition_logic):
    zip_conditions = [conditions[0]]
    for condition in conditions[1:]:
        zipped = 0
        for index in range(len(zip_conditions)):
            comp1, value1 = zip_conditions[index]['cond']['operation'], float(zip_conditions[index]['cond']['value'])
            comp2, value2 = condition['cond']['operation'], float(condition['cond']['value'])
            if comp1 == comp2:
                if (condition_logic == 'AND' and
                        ((comp1 in ['>=', '>'] and value1 <= value2) or
                         (comp1 in ['<=', '<'] and value1 >= value2))):
                    zip_conditions[index] = condition
                    zipped = 1
                    break
                if (condition_logic == 'OR' and
                        ((comp1 in ['>=', '>'] and value1 >= value2) or
                         (comp1 in ['<=', '<'] and value1 <= value2))):
                    zip_conditions[index] = condition
                    zipped = 1
                    break
        if zipped == 0:
            zip_conditions.append(condition)
    return zip_conditions


def get_priority_and(condition):
    operation_priority = {'=': 5, '<': 3, '>': 3, '<=': 1, '>=': 1}
    comp = condition['cond']['operation']
    return operation_priority.get(comp, None)


def get_priority_or(condition):
    operation_priority = {'<=': 5, '>=': 5, '<': 3, '>': 3, '=': 1}
    comp = condition['cond']['operation']
    return operation_priority.get(comp, None)


def calculate_priority(condition, condition_logic):
    if condition_logic == 'AND':
        return get_priority_and(condition)
    elif condition_logic == 'OR':
        return get_priority_or(condition)
    else:
        print(f"Error! Unknown condition type: {condition_logic}")
        return None


def condition_ordering(conditions, condition_logic):
    if condition_logic == 'AND':
        conditions_ordered = sorted(conditions, key=lambda x: calculate_priority(x, 'AND'), reverse=True)
        return conditions_ordered
    if condition_logic == 'OR':
        conditions_ordered = sorted(conditions, key=lambda x: calculate_priority(x, 'OR'), reverse=True)
        return conditions_ordered


def condition_optimizer(conditions, condition_logic):
    optimized_condition = zipping_condition(conditions, condition_logic)
    optimized_condition = condition_ordering(optimized_condition, condition_logic)
    return optimized_condition


def merge_dict(result, res1, table):
    # merge first table into empty dict
    for k, v in res1.items():
        # if result.get(k, False):
        #     continue
        # result[k] = v
        result[f"{table}.{k}"] = v
    return result


def merge_data(table1: dict, table2: dict, col1: str, col2: str, prefix1: str, prefix2: str, op='=') -> dict:
    result = {}
    for i1, item1 in enumerate(table1[col1]):
        for i2, item2 in enumerate(table2[col2]):
            if op == '=':
                if item2 == item1:
                    for key, value in table1.items():
                        if not result.get(f'{prefix1}.{key}'):
                            result[f'{prefix1}.{key}'] = []
                        result[f'{prefix1}.{key}'].append(value[i1])
                    for key, value in table2.items():
                        if not result.get(f'{prefix2}.{key}'):
                            result[f'{prefix2}.{key}'] = []
                        result[f'{prefix2}.{key}'].append(value[i2])
            elif op == '<':
                if item2 < item1:
                    for key, value in table1.items():
                        if not result.get(f'{prefix1}.{key}'):
                            result[f'{prefix1}.{key}'] = []
                        result[f'{prefix1}.{key}'].append(value[i1])
                    for key, value in table2.items():
                        if not result.get(f'{prefix2}.{key}'):
                            result[f'{prefix2}.{key}'] = []
                        result[f'{prefix2}.{key}'].append(value[i2])
            elif op == '<=':
                if item2 <= item1:
                    for key, value in table1.items():
                        if not result.get(f'{prefix1}.{key}'):
                            result[f'{prefix1}.{key}'] = []
                        result[f'{prefix1}.{key}'].append(value[i1])
                    for key, value in table2.items():
                        if not result.get(f'{prefix2}.{key}'):
                            result[f'{prefix2}.{key}'] = []
                        result[f'{prefix2}.{key}'].append(value[i2])
            elif op == '>':
                if item2 > item1:
                    for key, value in table1.items():
                        if not result.get(f'{prefix1}.{key}'):
                            result[f'{prefix1}.{key}'] = []
                        result[f'{prefix1}.{key}'].append(value[i1])
                    for key, value in table2.items():
                        if not result.get(f'{prefix2}.{key}'):
                            result[f'{prefix2}.{key}'] = []
                        result[f'{prefix2}.{key}'].append(value[i2])
            elif op == '>=':
                if item2 >= item1:
                    for key, value in table1.items():
                        if not result.get(f'{prefix1}.{key}'):
                            result[f'{prefix1}.{key}'] = []
                        result[f'{prefix1}.{key}'].append(value[i1])
                    for key, value in table2.items():
                        if not result.get(f'{prefix2}.{key}'):
                            result[f'{prefix2}.{key}'] = []
                        result[f'{prefix2}.{key}'].append(value[i2])
            elif op == '<>':
                if item2 != item1:
                    for key, value in table1.items():
                        if not result.get(f'{prefix1}.{key}'):
                            result[f'{prefix1}.{key}'] = []
                        result[f'{prefix1}.{key}'].append(value[i1])
                    for key, value in table2.items():
                        if not result.get(f'{prefix2}.{key}'):
                            result[f'{prefix2}.{key}'] = []
                        result[f'{prefix2}.{key}'].append(value[i2])
            else:
                print('Wrong operation!')
                return

    return result

def merge_sort_data(table1: dict, table2: dict, col1: str, col2: str, prefix1: str, prefix2: str, op='=', tag=0) -> dict:
    # Sort table1 (tim_sort : better than quick_sort & merge sort)
    table1_keys = list(table1.keys())
    table1_unsorted = list(zip(*table1.values()))
    table1_list_sorted = sorted(table1_unsorted, key=lambda x: x[table1_keys.index(col1)])
    table1_sorted = {key: [item[i] for item in table1_list_sorted] for i, key in enumerate(table1_keys)}

    # Sort table2 (tim_sort : better than quick_sort & merge sort)
    table2_keys = list(table2.keys())
    table2_unsorted = list(zip(*table2.values()))
    table2_list_sorted = sorted(table2_unsorted, key=lambda x: x[table2_keys.index(col2)])
    table2_sorted = {key: [item[i] for item in table2_list_sorted] for i, key in enumerate(table2_keys)}

    prefix_temp, col_temp, table_temp = prefix1, col1, table1_sorted
    prefix1, col1, table1_sorted = prefix2, col2, table2_sorted
    prefix2, col2, table2_sorted = prefix_temp, col_temp, table_temp
    # print(f"table1: {prefix1}, col1: {col1} {table1_sorted}")
    # print(f"table2: {prefix2}, col1: {col2} {table2_sorted}")
    # print(op)

    # Compare Value
    result = {}
    i, j = 0, 0
    if op == '=':
        while i < len(table1_sorted[col1]) and j < len(table2_sorted[col2]):
            if table1_sorted[col1][i] == table2_sorted[col2][j]:
                for key1 in table1_sorted.keys():
                    result_key = f"{prefix1}.{key1}"
                    if result_key not in result:
                        result[result_key] = []
                    result[result_key].append(table1_sorted[key1][i])
                for key2 in table2_sorted.keys():
                    result_key = f"{prefix2}.{key2}"
                    if result_key not in result:
                        result[result_key] = []
                    result[result_key].append(table2_sorted[key2][j])
                temp_j = j + 1
                while temp_j < len(table2_sorted[col2]) and table1_sorted[col1][i] == table2_sorted[col2][temp_j]:
                    for key1 in table1_sorted.keys():
                        result_key = f"{prefix1}.{key1}"
                        result[result_key].append(table1_sorted[key1][i])
                    for key2 in table2_sorted.keys():
                        result_key = f"{prefix2}.{key2}"
                        result[result_key].append(table2_sorted[key2][temp_j])
                    temp_j += 1

                i += 1
            elif table1_sorted[col1][i] < table2_sorted[col2][j]:
                i += 1
            else:
                j += 1
    if op == '>=':
        flag = 0
        while i < len(table1_sorted[col1]) and j < len(table2_sorted[col2]):
            # print(f"i: {i} j: {j} | t1 {table1_sorted[col1][i]}  t2 {table2_sorted[col2][j]}")
            if table1_sorted[col1][i] >= table2_sorted[col2][j]:
                for key1 in table1_sorted.keys():
                    result_key = f"{prefix1}.{key1}"
                    if result_key not in result:
                        result[result_key] = []
                    for index in range(flag, j+1):
                        result[result_key].append(table1_sorted[key1][i])
                for key2 in table2_sorted.keys():
                    result_key = f"{prefix2}.{key2}"
                    if result_key not in result:
                        result[result_key] = []
                    # result[result_key].append(table2_sorted[key2][j])
                    result[result_key].extend(table2_sorted[key2][flag:j+1])
                j += 1
                flag = j
            else:
                i += 1
                flag = 0
        if j == len(table2_sorted[col2]):
            i += 1
            while i < len(table1_sorted[col1]):
                for key1 in table1_sorted.keys():
                    result_key = f"{prefix1}.{key1}"
                    for index in range(0, len(table2_sorted[col2])):
                        result[result_key].append(table1_sorted[key1][i])
                for key2 in table2_sorted.keys():
                    result_key = f"{prefix2}.{key2}"
                    if result_key not in result:
                        result[result_key] = []
                    result[result_key].extend(table2_sorted[key2])
                i += 1
    if op == '>':
        flag = 0
        while i < len(table1_sorted[col1]) and j < len(table2_sorted[col2]):
            # print(f"i: {i} j: {j} | t1 {table1_sorted[col1][i]}  t2 {table2_sorted[col2][j]}")
            if table1_sorted[col1][i] > table2_sorted[col2][j]:
                for key1 in table1_sorted.keys():
                    result_key = f"{prefix1}.{key1}"
                    if result_key not in result:
                        result[result_key] = []
                    for index in range(flag, j+1):
                        result[result_key].append(table1_sorted[key1][i])
                for key2 in table2_sorted.keys():
                    result_key = f"{prefix2}.{key2}"
                    if result_key not in result:
                        result[result_key] = []
                    # result[result_key].append(table2_sorted[key2][j])
                    result[result_key].extend(table2_sorted[key2][flag:j+1])
                j += 1
                flag = j
            else:
                i += 1
                flag = 0
        if j == len(table2_sorted[col2]):
            i += 1
            while i < len(table1_sorted[col1]):
                for key1 in table1_sorted.keys():
                    result_key = f"{prefix1}.{key1}"
                    for index in range(0, len(table2_sorted[col2])):
                        result[result_key].append(table1_sorted[key1][i])
                for key2 in table2_sorted.keys():
                    result_key = f"{prefix2}.{key2}"
                    if result_key not in result:
                        result[result_key] = []
                    result[result_key].extend(table2_sorted[key2])
                i += 1
    if op == '<=':
        while i < len(table1_sorted[col1]) and j < len(table2_sorted[col2]):
            if table1_sorted[col1][i] <= table2_sorted[col2][j]:
                for key1 in table1_sorted.keys():
                    result_key = f"{prefix1}.{key1}"
                    if result_key not in result:
                        result[result_key] = []
                    for index in range(j, len(table2_sorted[col2])):
                        result[result_key].append(table1_sorted[key1][i])
                for key2 in table2_sorted.keys():
                    result_key = f"{prefix2}.{key2}"
                    if result_key not in result:
                        result[result_key] = []
                    # result[result_key].append(table2_sorted[key2][j])
                    result[result_key].extend(table2_sorted[key2][j:])
                i += 1
            else:
                j += 1
    if op == '<':
        while i < len(table1_sorted[col1]) and j < len(table2_sorted[col2]):
            if table1_sorted[col1][i] < table2_sorted[col2][j]:
                for key1 in table1_sorted.keys():
                    result_key = f"{prefix1}.{key1}"
                    if result_key not in result:
                        result[result_key] = []
                    for index in range(j, len(table2_sorted[col2])):
                        result[result_key].append(table1_sorted[key1][i])
                for key2 in table2_sorted.keys():
                    result_key = f"{prefix2}.{key2}"
                    if result_key not in result:
                        result[result_key] = []
                    # result[result_key].append(table2_sorted[key2][j])
                    result[result_key].extend(table2_sorted[key2][j:])
                i += 1
            else:
                j += 1
    return result


def merge_sort_data(table1: dict, table2: dict, col1: str, col2: str, prefix1: str, prefix2: str, op = '=') -> dict:
    # Sort table1 (tim_sort : better than quick_sort & merge sort)
    table1_keys = list(table1.keys())
    table1_unsorted = list(zip(*table1.values()))
    table1_list_sorted = sorted(table1_unsorted, key=lambda x: x[table1_keys.index(col1)])
    table1_sorted = {key: [item[i] for item in table1_list_sorted] for i, key in enumerate(table1_keys)}
    # print(table1_sorted)

    # Sort table2 (tim_sort : better than quick_sort & merge sort)
    table2_keys = list(table2.keys())
    table2_unsorted = list(zip(*table2.values()))
    table2_list_sorted = sorted(table2_unsorted, key=lambda x: x[table2_keys.index(col2)])
    table2_sorted = {key: [item[i] for item in table2_list_sorted] for i, key in enumerate(table2_keys)}
    # print(table2_sorted)

    # Compare Value
    result = {}
    i, j = 0, 0
    while i < len(table1_sorted[col1]) and j < len(table2_sorted[col2]):
        if table1_sorted[col1][i] == table2_sorted[col2][j]:
            for key1 in table1_sorted.keys():
                result_key = f"{prefix1}.{key1}"
                if result_key not in result:
                    result[result_key] = []
                result[result_key].append(table1_sorted[key1][i])
            for key2 in table2_sorted.keys():
                result_key = f"{prefix2}.{key2}"
                if result_key not in result:
                    result[result_key] = []
                result[result_key].append(table2_sorted[key2][j])
            temp_j = j + 1
            while temp_j < len(table2_sorted[col2]) and table1_sorted[col1][i] == table2_sorted[col2][temp_j]:
                for key1 in table1_sorted.keys():
                    result_key = f"{prefix1}.{key1}"
                    result[result_key].append(table1_sorted[key1][i])
                for key2 in table2_sorted.keys():
                    result_key = f"{prefix2}.{key2}"
                    result[result_key].append(table2_sorted[key2][temp_j])
                temp_j += 1

            i += 1
        elif table1_sorted[col1][i] < table2_sorted[col2][j]:
            i += 1
        else:

            j += 1

    return result




