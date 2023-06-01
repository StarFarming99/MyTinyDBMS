import re
from re import compile
from tokenize import group


def check_word_in_string(string, word):
    pattern = re.compile(r'\b' + word + r'\b')
    result = re.search(pattern, string)

    return result is not None


class Parser:
    def __init__(self):
        self.p_operation_map = {
            'SELECT': self.p_select,
            'UPDATE': self.p_update,
            'DELETE': self.p_delete,
            'INSERT': self.p_insert,
            'USE': self.p_use,
            'CREATE': self.p_create,
            'SHOW': self.p_show,
            'DROP': self.p_drop,
            'JOIN': self.p_join,
        }
        self.p_re_map = {
            'CREATE': r'(CREATE|create) (TABLE|table) (.*?) *\((.*(?:PRIMARY KEY|FOREIGN KEY).*)\)',
            'CREATE INDEX': r'(CREATE|create) (INDEX|index) (.*) (ON|on) (.*) \((.*)\)',
            'DROP INDEX': r'(DROP|drop) (INDEX|index) (.*) (ON|on) (.*)',
            'CREATE DATABASE': r'(CREATE|create) (DATABASE|database) (.*)',
            'SELECT': r'(SELECT|select) (.*) (FROM|from) (.*)',
            'UPDATE': r'(UPDATE|update) (.*) (SET|set) (.*)',
            'DELETE': r'(DELETE|delete) (FROM|from) (.*)',
            'INSERT': r'(INSERT|insert) (INTO|into) (.*) (VALUES|values) \((.*)\)',
            'GROUPBY': r'(.*) (GROUP|group) (BY|by) (.*)'
        }

    def p_remove_space(self, obj):
        ret = []
        for x in obj:
            if x.strip() == '':
                continue
            ret.append(x)
        return ret

    def p_parse(self, statement):
        if 'where' in statement:
            statement = statement.split("where")
        else:
            statement = statement.split("WHERE")

        base_statement = self.p_remove_space(statement[0].split(" "))

        if len(base_statement) < 2 and base_statement[0].lower() not in ['exit', 'show']:
            print('Syntax Error for: %s' % statement)
            return

        if "JOIN" in base_statement or "join" in base_statement:
            action_type = "JOIN"
        else:
            action_type = base_statement[0].upper()

        if action_type not in self.p_operation_map:
            print('Syntax Error for: %s' % statement)
            return

        action = self.p_operation_map[action_type](base_statement)

        if action is None or 'type' not in action:
            print('Syntax Error for: %s' % statement)
            return None

        conditions = []
        if len(statement) == 2:
            if 'ORDER BY' in statement[1]:
                action['orderby'] = statement[1].split('ORDER BY')[1].strip().split(' ')[0]
                statement[1] = statement[1].replace('ORDER BY ' + action['orderby'], '')
            elif 'order by' in statement[1]:
                action['orderby'] = statement[1].split('order by')[1].strip().split(' ')[0]
                statement[1] = statement[1].replace('ORDER BY ' + action['orderby'], '')
            if 'GROUP BY' in statement[1]:
                sub_statement = statement[1].split('GROUP BY')
            else:
                sub_statement = statement[1].split('group by')
            if len(sub_statement) > 1:
                if 'limit' in sub_statement[1]:
                    sub_sub_statement = sub_statement[1].split('limit')
                else:
                    sub_sub_statement = sub_statement[1].split('LIMIT')
                if len(sub_sub_statement) == 1:
                    action['groupby'] = sub_statement[1].strip()
                else:
                    action['groupby'] = sub_sub_statement[0].strip()
                    action['limit'] = sub_sub_statement[1].strip()
            else:
                if 'LIMIT' in sub_statement[0]:
                    action['limit'] = sub_statement[0].split('LIMIT')[1].strip().split(' ')[0]
                    sub_statement[0] = sub_statement[0].replace('LIMIT ' + action['limit'], '')
                elif 'limit' in sub_statement[0]:
                    action['limit'] = sub_statement[0].split('limit')[1].strip().split(' ')[0]
                    sub_statement[0] = sub_statement[0].replace('limit ' + action['limit'], '')

            if check_word_in_string(sub_statement[0].lower(), 'and'):
                conditions_list = self.p_remove_space(sub_statement[0].split("AND"))
                action['condition_logic'] = 'AND'
                for cond in conditions_list:
                    conditions.extend(self.p_remove_space(cond.split(" ")))
            elif check_word_in_string(sub_statement[0].lower(), 'or'):
                # print(sub_statement[0])
                conditions_list = self.p_remove_space(sub_statement[0].split("OR"))
                action['condition_logic'] = 'OR'
                for cond in conditions_list:
                    conditions.extend(self.p_remove_space(cond.split(" ")))
            else:
                conditions.extend(self.p_remove_space(sub_statement[0].split(" ")))

        if conditions:
            if len(conditions) < 3:
                print('Cannot Resolve Given Input!!!')
                return
            action['conditions'] = []  # conditions 条件
            for index in range(0, len(conditions), 3):
                field = conditions[index]
                symbol = conditions[index + 1].upper()
                condition = conditions[index + 2]
                action['conditions'].append({
                    'field': field,
                    "cond": {
                        'operation': symbol,
                        'value': condition
                    }
                })
        return action

    def p_compare(self, action):
        return compile(self.p_re_map[action])

    def p_join(self, statement):
        comp = self.p_compare('SELECT')
        ret = comp.findall(' '.join(statement))[0]
        if ret and len(ret) == 4:
            fields = ret[1]
            join_fields = {}
            left = ret[3].split(" ")
            join_field = [left[-1], left[-3]]
            op = left[-2]
            for str in join_field:
                table = str.split(".")[0]
                col = str.split(".")[1]
                join_fields[table] = col
            if fields != '*':
                fields = [field.strip() for field in fields.split(',')]
            return {
                'type': 'search join',
                'join type': left[1],
                'tables': left[0],
                'fields': fields,
                'join fields': join_fields,
                'operation': op
            }

    def p_select(self, statement):
        comp = self.p_compare('SELECT')
        ret = comp.findall(' '.join(statement))
        if ret and len(ret[0]) == 4:
            comp = self.p_compare('GROUPBY')
            groupby = comp.findall(ret[0][3])

            fields = ret[0][1]
            if fields != '*':
                fields = [field.strip() for field in fields.split(',')]

            action = {
                'type': 'search',
                'fields': fields
            }

            if groupby:
                # print(groupby)
                action['table'] = groupby[0][0]
                if len(groupby[0][3]) == 1:
                    action['groupby'] = groupby[0][3]
                    action['groupby_condition'] = [-1, -1, -1]
                else:
                    action['groupby'] = groupby[0][3].split(' ')[0]
                    action['groupby_condition'] = [-1, -1, -1]
                str_group = groupby[0][3]
                if 'LIMIT' in str_group.upper():
                    tmp = str_group.split('LIMIT')
                    if tmp[1].strip().isdigit():
                        action['groupby_condition'][2] = int(tmp[1].strip())
                    else:
                        print("Please Provide Integer as LIMIT Constraint!!!")
                        return
                    index = str_group.index('LIMIT')
                    str_group = str_group[:index]
                if 'ORDER BY' in str_group.upper():
                    tmp = str_group.split('ORDER BY')
                    action['groupby_condition'][1] = tmp[1].strip()
                    index = str_group.index('ORDER BY')
                    str_group = str_group[:index]
                if 'HAVING' in str_group.upper():
                    tmp = str_group.split('HAVING')
                    action['groupby_condition'][0] = tmp[1].strip()

            else:
                if 'ORDER' in statement:
                    index = statement.index('BY')
                    action['orderby'] = statement[index + 1]
                elif 'order' in statement:
                    index = statement.index('by')
                    action['orderby'] = statement[index + 1]
                try:
                    if 'limit' in ret[0][3]:
                        action['limit'] = int(
                            ret[0][3].split('LIMIT')[1].split('order by')[0].split('ORDER BY')[0].strip())
                        action['table'] = ret[0][3].split('limit')[0].split('order by')[0].split('ORDER BY')[0].strip()
                    elif 'LIMIT' in ret[0][3]:
                        action['limit'] = int(
                            ret[0][3].split('LIMIT')[1].split('order by')[0].split('ORDER BY')[0].strip())
                        action['table'] = ret[0][3].split('LIMIT')[0].split('order by')[0].split('ORDER BY')[0].strip()
                    else:
                        action['table'] = ret[0][3].split(' ')[0]
                except Exception:
                    print("Please Provide Integer as LIMIT Constraint!!!")

            return action
        return None

    def p_update(self, statement):
        comp = self.p_compare('UPDATE')
        ret = comp.findall(' '.join(statement))

        if ret and len(ret[0]) == 4:
            data = {
                'type': 'update',
                'table': ret[0][1],
                'data': {}
            }
            set_statement = ret[0][3].split(',')
            for s in set_statement:
                s = s.split('=')
                field = s[0].strip()
                value = s[1].strip()
                if "'" in value or '"' in value:
                    value = value.replace('"', '').replace(",", '').strip()
                else:
                    try:
                        value = value.strip()
                    except:
                        return None
                data['data'][field] = value
            return data
        return None

    def p_delete(self, statement):
        return {
            'type': 'delete',
            'table': statement[2]
        }

    def p_insert(self, statement):
        ret = self.p_compare('INSERT').findall(' '.join(statement))
        if ret and len(ret[0]) == 5:
            ret_tmp = ret[0]
            # check if the given table name is a string without space, raise error if do contain space
            if len(ret_tmp[2].split(' ')) > 1:
                return None
            values = ret_tmp[4].split(", ")
            data = {
                'type': 'insert',
                'table': ret_tmp[2],
                'values': values
            }
            return data

        return None

    def p_use(self, statement):
        return {
            'type': 'use',
            'database': statement[1]
        }

    def p_create(self, statement):
        comp = self.p_compare('CREATE DATABASE')
        ret = comp.findall(' '.join(statement))
        if ret:
            info = {
                'type': 'create_db',
                'name': ret[0][2]
            }
            return info

        comp = self.p_compare('CREATE')
        ret = comp.findall(' '.join(statement))
        # check if the values and definition is provided
        if ret:
            info = {}
            info['type'] = 'create'
            info['name'] = statement[2]
            info['cols'] = {}
            vars = ret[0][3].split(',')
            for var_type in vars:
                detailed = var_type.strip().split(' ')
                if len(detailed) == 2:
                    info['cols'][detailed[0]] = []
                    for i in range(1, len(detailed)):
                        info['cols'][detailed[0]].append(detailed[i])
                elif len(detailed) > 2:
                    if detailed[0] == 'PRIMARY':
                        info['primary key'] = detailed[2][1:len(detailed[2]) - 1]
                    if detailed[0] == 'FOREIGN':
                        l_index = detailed[4].index('(')
                        r_index = detailed[4].index(')')
                        info['foreign key'] = [detailed[2][1:len(detailed[2]) - 1], detailed[4][:l_index],
                                               detailed[4][l_index + 1:r_index]]
            return info

        comp = self.p_compare('CREATE INDEX')
        ret = comp.findall(' '.join(statement))
        if ret:
            info = {
                'type': 'create_index',
                'table': ret[0][4],
                'name': ret[0][2],
                'col': ret[0][5]
            }
            return info

        print("Cannot Resolve Given Input!!!")
        return None

    def p_show(self, statement):
        kind = statement[1]

        if kind.upper() == 'DATABASES':
            return {
                'type': 'show',
                'kind': 'databases'
            }
        if kind.upper() == 'TABLES':
            return {
                'type': 'show',
                'kind': 'tables'
            }

    def p_drop(self, statement):
        kind = statement[1]
        if len(statement) < 3:
            print("ERROR!!! Cannot Resolve Given Input!")
            return
        elif kind.upper() == 'DATABASE':
            return {
                'type': 'drop',
                'kind': 'database',
                'name': statement[2]
            }
        elif kind.upper() == 'TABLE':
            return {
                'type': 'drop',
                'kind': 'table',
                'name': statement[2]
            }
        elif kind.upper() == 'INDEX':
            comp = self.p_compare('DROP INDEX')
            ret = comp.findall(' '.join(statement))
            if ret:
                return {
                    'type': 'drop',
                    'kind': 'index',
                    'name': statement[2],
                    'table': ret[0][4]
                }
        print("ERROR!!! Cannot Resolve Given Input!")
        return


if __name__ == '__main__':
    my_instance = Parser()
    statement = 'SELECT * FROM i-i-100000 JOIN i-i-1000 ON i-i-100000.c1 > i-i-1000.c1'
    statement1 = 'SELECT * FROM t2 WHERE id > 3 ORDER BY value'
    statement2 = 'CREATE TABLE t2 (id INT, t1_id INT, name STRING, value int, PRIMARY KEY (id), FOREIGN KEY (t1_id) REFERENCES t1(id))'
    statement12 = 'SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t1.id < 3 AND t1.id > 1'
    statement3 = 'SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id < 3 AND t1.id > 1'
    statement3 = 'SELECT SUM(value) FROM t2 GROUP BY t1_id HAVING SUM(value) > 50 AND SUM(value) < 70'
    statement3 = 'SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id'
    statement4 = 'SELECT * FROM t2 WHERE id <= 3 OR id >= 6 '
    action = my_instance.p_parse(statement4)
    print(action)
