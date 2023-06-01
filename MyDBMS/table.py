from bplus_tree import BPlusTree
from re import findall
import re
import optimizer
import sys
from optimizer import *


def get_range(s):
    column = s.strip().split()[0]
    if column not in s:
        raise ValueError(f"{column} not found in the input string.")

    operators = re.findall(r'(?:<=|>=|<|>|=)', s)
    values = re.findall(r'\d+', s)
    logical_operators = re.findall(r'(?:AND|OR)', s)

    if len(logical_operators) > 1:
        raise ValueError('More than one logical operator found.')

    ranges = []
    logical_op = None

    if logical_operators:
        logical_op = logical_operators[0]

    if logical_op == "AND":
        lower_bound = float('-inf')
        upper_bound = float('inf')

        for op, value in zip(operators, values):
            if op == '<':
                upper_bound = float(value) - 1
            elif op == '>':
                lower_bound = float(value) + 1
            elif op == '<=':
                upper_bound = float(value)
            elif op == '>=':
                lower_bound = float(value)
            else:
                raise ValueError('Invalid operator found.')

        ranges.append((lower_bound, upper_bound))
    elif logical_op == "OR":
        for op, value in zip(operators, values):
            if op == '<':
                ranges.append((float('-inf'), float(value) - 1))
            elif op == '>':
                ranges.append((float(value) + 1, float('inf')))
            elif op == '<=':
                ranges.append((float('-inf'), float(value)))
            elif op == '>=':
                ranges.append((float(value), float('inf')))
            else:
                raise ValueError('Invalid operator found.')
    else:
        raise ValueError('Invalid logical operator found.')

    return ranges

class Table:
    def __init__(self, name, var_type):
        self.name = name
        self.var = []
        self.type = []
        self.t_init_var_type(var_type)
        self.data = {}
        self.statistics = {}
        self.btrees = {}
        self.primary = None
        self.foreign = {}
        # init the list for each columns
        for col in self.var:
            self.data[col] = []
            if self.type[self.var.index(col)][0] == 'int':
                self.statistics[col] = {"MIN": 2147483647, "MAX": -2147483648}
            elif self.type[self.var.index(col)][0] == 'float':
                self.statistics[col] = {"MIN": float('inf'), "MAX": float('-inf')}
            else:
                self.statistics[col] = {"MIN": 50 * "a", "MAX": ""}
        self.t_condition_map = {
            '=': self.t_equal,
            '>': self.t_bigger,
            '<': self.t_smaller,
            '>=': self.t_bigger_and_equal,
            '<=': self.t_smaller_and_equal,
        }
        self.t_select_filter_map = {
            'avg': self.t_select_avg,
            'count': self.t_select_count,
            'max': self.t_select_max,
            'min': self.t_select_min,
            'sum': self.t_select_sum,
        }

    def t_filter(self, cond, col):
        try:
            return self.t_condition_map[cond["operation"]](cond, col)
        except Exception:
            print('Error! Cannot Resolve Given Input111')
            return

    def t_format(self, col, value):
        if self.type[self.var.index(col)][0] == 'int':
            return int(value)
        elif self.type[self.var.index(col)][0] == 'float':
            return float(value)
        return value

    def t_equal(self, cond, col):
        if col in self.btrees.keys():
            indexs = self.btrees[col]['tree'].search('=', self.t_format(col, cond["value"]))
            return indexs
        if cond["value"].isdigit():
            return [index for index, v in enumerate(self.data[col]) if v == float(self.t_format(col, cond["value"]))]
            #return optimizer.get_equal_keys_list(self.data[col], self.t_format(col, cond["value"]))
        else:
            return [index for index, v in enumerate(self.data[col]) if v == self.t_format(col, cond["value"])]
            #return util.get_equal_keys_list1(self.data[col], self.t_format(col, cond["value"]))

    def t_bigger(self, cond, col):
        if col in self.btrees.keys():
            indexs = self.btrees[col]['tree'].search('>', self.t_format(col, cond["value"]))
            return indexs
        return [index for index, v in enumerate(self.data[col]) if v > float(self.t_format(col, cond["value"]))]
        #return util.get_more_keys_list(self.data[col], self.t_format(col, cond["value"]))

    def t_smaller(self, cond, col):
        if col in self.btrees.keys():
            indexs = self.btrees[col]['tree'].search('<', self.t_format(col, cond["value"]))
            return indexs
        #return util.get_less_keys_list(self.data[col], self.t_format(col, cond["value"]))
        return [index for index, v in enumerate(self.data[col]) if v < float(self.t_format(col, cond["value"]))]

    def t_bigger_and_equal(self, cond, col):
        if col in self.btrees.keys():
            indexs = self.btrees[col]['tree'].search('>=', self.t_format(col, cond["value"]))
            return indexs
        #return util.get_more_equal_keys_list(self.data[col], self.t_format(col, cond["value"]))
        return [index for index, v in enumerate(self.data[col]) if v >= float(self.t_format(col, cond["value"]))]

    def t_smaller_and_equal(self, cond, col):
        if col in self.btrees.keys():
            indexs = self.btrees[col]['tree'].search('<=', self.t_format(col, cond["value"]))
            return indexs
        #return util.get_less_equal_keys_list(self.data[col], self.t_format(col, cond["value"]))
        return [index for index, v in enumerate(self.data[col]) if v <= float(self.t_format(col, cond["value"]))]

    def t_update_index(self):
        self.btrees[self.primary]['tree'].insert(self.data[self.primary][-1], len(self.data[self.primary]) - 1)

    def t_update_index_update(self):
        if self.btrees == {}:
            return
        for name in self.btrees.keys():
            self.btrees[name]['tree'] = BPlusTree()
            for i in range(len(self.data[name])):
                self.btrees[name]['tree'].insert(self.data[name][i], i)

    def t_init_var_type(self, var_type):
        for var, type in var_type.items():
            self.var.append(var)
            self.type.append(type)

    def t_delete_data(self, index_delete):
        # sort the index list in decending order so we can remove all in once without error
        index_delete.sort(reverse=True)
        for index in index_delete:
            for col in self.var:
                del self.data[col][index]

    def t_select_avg(self, field, index):
        _sum = 0
        for i in index:
            _sum += self.data[field][i]

        return [_sum / len(index)]

    def t_select_count(self, field, index):
        return [len(index)]

    def t_select_max(self, field, index):
        _max = self.data[field][index[0]]
        for i in index:
            if _max < self.data[field][i]:
                _max = self.data[field][i]
        return [_max]

    def t_select_min(self, field, index):
        _min = self.data[field][index[0]]
        for i in index:
            if _min > self.data[field][i]:
                _min = self.data[field][i]

        return [_min]

    def t_select_sum(self, field, index):
        _sum = 0
        for i in index:
            _sum += self.data[field][i]
        return [_sum]

    # a helper function used to help select function to get corresponding info
    def t_select_data(self, index_select, fields):
        try:
            result = dict()
            for index in index_select:
                for field in fields:
                    if not result.get(field, False):
                        result[field] = []
                    result[field].append(self.data[field][index])
            return result
        except Exception:
            print("key error!")

    def t_select_data_filter(self, index_select, fields, filter):
        result = dict()
        for i in range(len(fields)):
            if fields[i] == '*':
                field = self.primary
            else:
                field = fields[i]
            if filter[i] in self.t_select_filter_map.keys():
                result[f"{filter[i]}_{fields[i]}"] = self.t_select_filter_map[filter[i]](field, index_select)
        return result

    def t_select_data_groupby(self, index_select, fields, filter, groupby=None, groupby_condition=None):
        col_set = list(set(self.data[groupby]))
        col_select = {}
        for v in col_set:
            col_select[v] = []
        for i in index_select:
            for v in col_set:
                if self.data[groupby][i] == v:
                    col_select[v].append(i)
        result = {
            groupby: col_set
        }

        for col in col_set:
            for i in range(len(fields)):
                if result.get(filter[i].upper() + '(' + fields[i] + ')') == None:
                    result[filter[i].upper() + '(' + fields[i] + ')'] = []
                if col_select[col] == []:
                    result[filter[i].upper() + '(' + fields[i] + ')'].append(0)
                else:
                    result[filter[i].upper() + '(' + fields[i] + ')'].append(
                        self.t_select_filter_map[filter[i]](fields[i], col_select[col])[0])

        if groupby_condition[0] != -1:
            temp = groupby_condition[0].strip().split(' ')
            if len(temp) == 3:
                c = temp[0]
                op = temp[1]
                v = int(temp[2])
                if c not in result.keys():
                    print("Wrong HAVING")
                    return
                if op == '=':
                    target = v
                    filtered_data = {
                        key: [value for v, value in zip(result[c], values) if v == target]
                        for key, values in result.items()
                    }
                    result = filtered_data
                if op == '<':
                    low = float('-inf')
                    high = v-1
                    filtered_data = {
                        key: [value for v, value in zip(result[c], values) if low <= v and v <= high]
                        for key, values in result.items()
                    }
                    result = filtered_data
                if op == '>':
                    low = v+1
                    high = float('inf')
                    filtered_data = {
                        key: [value for v, value in zip(result[c], values) if low <= v and v <= high]
                        for key, values in result.items()
                    }
                    result = filtered_data
                if op == '<=':
                    low = float('-inf')
                    high = v
                    filtered_data = {
                        key: [value for v, value in zip(result[c], values) if low <= v and v <= high]
                        for key, values in result.items()
                    }
                    result = filtered_data
                if op == '>=':
                    low = v
                    high = float('inf')
                    filtered_data = {
                        key: [value for v, value in zip(result[c], values) if low <= v and v <= high]
                        for key, values in result.items()
                    }
                    result = filtered_data
                if op == '<>':
                    i = result[c].index(v)
                    for k, values in result.items():
                        result[k] = [x for x in values if x != values[i]]

            elif len(temp) == 7:
                c1 = temp[0]
                c2 = temp[4]
                if c1 != c2:
                    print("NOT support logical judgement with different columns!")
                    return
                range11 = get_range(groupby_condition[0])
                # print(range11)
                if len(range11) == 1:
                    low = range11[0][0]
                    high = range11[0][1]
                    filtered_data = {
                        key: [value for v, value in zip(result[c1], values) if low <= v and v <= high]
                        for key, values in result.items()
                    }
                    result = filtered_data
                if len(range11) == 2:
                    low = range11[0][1]
                    high = range11[1][0]
                    filtered_data = {
                        key: [value for v, value in zip(result[c1], values) if v <= low or v >= high]
                        for key, values in result.items()
                    }
                    result = filtered_data
        if groupby_condition[1] != -1:
            sorted_indices = sorted(range(len(result[groupby_condition[1]])), key=lambda k: result[groupby_condition[1]][k])
            sorted_res = {}
            for key, value in result.items():
                sorted_res[key] = [value[i] for i in sorted_indices]
            result = sorted_res

        if groupby_condition[2] != -1:

            sliced_res = {}
            for key, value in result.items():
                sliced_res[key] = value[:groupby_condition[2]]
            # print(sliced_res)
            result = sliced_res
        return result

    def t_get_var(self):
        return self.var

    def t_delete(self, action):
        if action.get('conditions'):
            cols_select = []
            conditions_select = []
            for condition in action["conditions"]:
                cols_select.append(condition['field'])
                conditions_select.append(condition['cond'])

            index_list_select = []
            for i in range(len(conditions_select)):
                cond = conditions_select[i]
                col = cols_select[i]
                if cond["operation"] not in self.t_condition_map:
                    print('Error! Cannot Resolve Given Input')
                    return
                tmp = self.t_filter(cond, col)
                index_list_select.append(tmp)
        else:
            print("ERROR! Cannot Resolve Given Input!")
            return

            # set a condition check for only one constraint
        if len(index_list_select) == 1:
            self.t_delete_data(index_list_select[0])
        else:
            index_select = index_list_select[0]
            if action['condition_logic'] == 'AND':
                # get intersection
                for i in range(1, len(index_list_select)):
                    index_select = list(set(index_select).intersection(index_list_select[i]))
                index_select.sort()
            elif action['condition_logic'] == 'OR':
                # get intersection
                for i in range(1, len(index_list_select)):
                    index_select = list(set(index_select).union(index_list_select[i]))
                index_select.sort()
            self.t_delete_data(index_select)
            return

    # Select By Conditions
    def t_select(self, action):
        if action.get('orderby'):
            if action['orderby'] not in self.var:
                print('Error! Cannot Resolve Given Column: ', action['orderby'])
                return
        if action['fields'] == '*':
            fields = self.var
            filter = None
        else:
            fields, filter = self.t_check_filter(action["fields"])

        if action.get('conditions') and len(action['conditions']) == 2:
            conditions = action['conditions']
            if action['condition_logic'] == 'AND':
                # Short-circuit evaluation for AND
                for condition in conditions:
                    comp_is_false = optimizer.is_return_false(condition, self.statistics)
                    if comp_is_false:
                        index_select = []
                        break
                else:
                    conditions_optimized = optimizer.condition_optimizer(conditions, 'AND')
                    index_list_select = []
                    for condition in conditions_optimized:
                        if condition['cond']["operation"] not in self.t_condition_map:
                            print('Error! Cannot Resolve Given Input')
                            return
                        index_list_select.append(self.t_filter(condition['cond'], condition['field']))
                        index_select = index_list_select[0]
                    for i in range(1, len(index_list_select)):
                        index_select = list(set(index_select).intersection(index_list_select[i]))
            elif action['condition_logic'] == 'OR':
                # Short-circuit evaluation for OR
                for condition in conditions:
                    comp_is_true = optimizer.is_return_true(condition, self.statistics)
                    if comp_is_true:
                        index_list_select = [[i for i in range(len(self.data[self.var[0]]))]]
                        index_select = index_list_select[0]
                        break
                else:
                    conditions_optimized = optimizer.condition_optimizer(conditions, 'OR')
                    index_list_select = []
                    for condition in conditions_optimized:
                        if condition['cond']["operation"] not in self.t_condition_map:
                            print('Error! Cannot Resolve Given Input')
                            return
                        index_list_select.append(self.t_filter(condition['cond'], condition['field']))
                        index_select = index_list_select[0]
                    for i in range(1, len(index_list_select)):
                        index_select = list(set(index_select).union(index_list_select[i]))
        elif action.get('conditions'):
            index_list_select = []
            for condition in action["conditions"]:
                if condition['cond']["operation"] not in self.t_condition_map:
                    print('Error! Cannot Resolve Given Input')
                    return
                index_list_select.append(self.t_filter(condition['cond'], condition['field']))
            index_select = index_list_select[0]
            if len(index_list_select) > 1:
                if action['condition_logic'] == 'AND':
                    # get intersection
                    for i in range(1, len(index_list_select)):
                        index_select = list(set(index_select).intersection(index_list_select[i]))
                elif action['condition_logic'] == 'OR':
                    # get intersection
                    for i in range(1, len(index_list_select)):
                        index_select = list(set(index_select).union(index_list_select[i]))
            # index_list_select = []
            # for condition in action["conditions"]:
            #     if condition['cond']["operation"] not in self.t_condition_map:
            #         print('Error! Cannot Resolve Given Input')
            #         return
            #     index_list_select.append(self.t_filter(condition['cond'], condition['field']))
            # index_select = index_list_select[0]
            # print(index_select)
        else:
            index_list_select = [[i for i in range(len(self.data[self.var[0]]))]]
            index_select = index_list_select[0]
        # set a condition check for only one constraint

        orderby = None
        if action.get('orderby'):
            orderby = []
            for i in index_select:
                orderby.append(self.data[action['orderby']][i])

        if filter and not filter == ['']:
            if "groupby" in action.keys():
                result = self.t_select_data_groupby(index_select, fields, filter, action['groupby'], action['groupby_condition'])
                return result, None, orderby
            else:
                result = self.t_select_data_filter(index_select, fields, filter)
                return result, None, None
        else:
            if action.get('groupby'):
                raise Exception("ERROR!!! Cannot Run 'GROUP BY' Without Constraint!")
            result = self.t_select_data(index_select, fields)

        type = {}
        for var in result.keys():
            type[var] = (self.type[self.var.index(var)])

        return result, type, orderby

    def t_insert_var(self, type, col, value):
        if type == 'int':
            value = int(value)
        elif type == 'float':
            value = float(value)
        if self.primary == col and value in self.data[self.primary]:
            raise Exception("ERROR!!! Duplicate Primary Key Value Exists!")
        self.data[col].append(value)
        if self.statistics[col]["MIN"] > value:
            self.statistics[col]["MIN"] = value
        if self.statistics[col]["MAX"] < value:
            self.statistics[col]["MAX"] = value

    def t_insert(self, action):
        # check the type of input, one is specify the columns they want to insert, other one does not
        # {'type': 'insert', 'table': 'table1', 'data': {'col1': 1, ' col2': 2, ' col3': 3, ' col4': 4}}
        # {'type': 'insert', 'table': 'table1', 'values': ['1', ' 2', ' 3', ' 4']}
        # inlcude data means this statement specified the columns
        if not action.get('data') == None:
            if action.get(self.primary) == None:
                raise Exception("ERROR!!! No Primary Value Provided!")
            for col in self.var:
                self.data[col].append(action.get(col))
        # otherwise, not
        else:
            # check if the provided columns matches
            if len(action['values']) != len(self.var):
                print('Can not resolve input')
            else:
                for i in range(len(action['values'])):
                    self.t_insert_var(self.type[i][0].lower(), self.var[i], action['values'][i])

        # check if the table got user defiend primary key, and append it
        if self.primary == 'index__':
            self.data[self.primary].append(self.index)
            self.index += 1

    def t_check_filter(self, fields):
        filter = []
        result = []
        if "(" not in fields[0]:
            return fields, ['']

        for field in fields:
            if "avg" in field.lower():
                filter.append('avg')
            elif "count" in field.lower():
                filter.append('count')
            elif "max" in field.lower():
                filter.append('max')
            elif "min" in field.lower():
                filter.append('min')
            elif "sum" in field.lower():
                filter.append('sum')
            else:
                filter.append("")
            result.append(findall(r"\((.*?)\)", field)[0])

        if "" in filter:
            for ff in filter:
                if ff != "":
                    raise Exception(f"ERROR!!! Cannot select both Column and {ff}")
        return result, filter

    def t_update(self, action):
        if not action.get('conditions'):
            print("ERROR! Cannot Resolve Given Input!!")
            return

        cols_select = []
        conditions_select = []
        for condition in action["conditions"]:
            cols_select.append(condition['field'])
            conditions_select.append(condition['cond'])
        index_list_select = []

        for i in range(len(conditions_select)):

            cond = conditions_select[i]
            col = cols_select[i]
            if cond["operation"] not in self.t_condition_map:
                print('Error! Cannot Resolve Given Input')
                return
            tmp = self.t_filter(cond, col)
            index_list_select.append(tmp)

        index_select = index_list_select[0]
        if "condition_logic" in action.keys():
            if action['condition_logic'] == 'AND':
                # get intersection
                for i in range(1, len(index_list_select)):
                    index_select = list(set(index_select).intersection(index_list_select[i]))
                index_select.sort()
            elif action['condition_logic'] == 'OR':
                # get intersection
                for i in range(1, len(index_list_select)):
                    index_select = list(set(index_select).union(index_list_select[i]))
                index_select.sort()

        for i in range(len(index_list_select)):
            tmp = [val for val in index_select if val in index_list_select[i]]

        data = action['data']
        for i in data.keys():
            for j in tmp:
                if self.t_is_number(data[i]):
                    if self.primary == i and int(data[i]) in self.data[self.primary]:
                        raise Exception("ERROR!!! Duplicate Primary Key Value Exists!")
                    self.data[i][j] = int(data[i])

                else:
                    if self.primary == i and data[i] in self.data[self.primary]:
                        raise Exception("ERROR!!! Duplicate Primary Key Value Exists!")
                    self.data[i][j] = data[i]

    # create a bplustree for the given column
    def t_create_index(self, action):
        # check if the columns is in this table
        if action['col'] not in self.var:
            print("ERROR! No Column Named '%s'" % (action['col']))
            return False
        # init the name and tree
        if action['col'] in self.btrees.keys():
            print('Already Exist index on %s' % (action['col']))
            return False
        self.btrees[action['col']] = {
            'name': action['name'],
            'tree': BPlusTree()
        }
        # insert index as value where value as key
        for i in range(len(self.data[action['col']])):
            self.btrees[action['col']]['tree'].insert(self.data[action['col']][i], i)

        return True

    def t_drop_index(self, action):
        cols = []
        for key, value in self.btrees.items():
            if value['name'] == action['name']:
                cols.append(key)
        if not cols == []:
            for col in cols:
                del self.btrees[col]
            return True
        return False

    def t_check_column(self, input_col):
        table_col = [*self.data]
        for ic in input_col:
            if ic not in table_col:
                print(f"Table does not have such column {ic}")
                return False
            else:
                return True

    def t_is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            pass

        try:
            import unicodedata
            unicodedata.numeric(s)
            return True
        except (TypeError, ValueError):
            pass

        return False
