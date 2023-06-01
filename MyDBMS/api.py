import math
from shutil import rmtree
from pickle import dump, load
import os
import copy
from parser import *
from table import *
from optimizer import *

class API:
    def __init__(self):
        self.parser = Parser()
        self.database = {}
        self.currentDB = None
        self.tables = None
        self.changed = []
        self.function = {
            'insert': self.a_insert,
            'create': self.a_create,
            'search': self.a_select,
            'delete': self.a_delete,
            'update': self.a_update,
            'create_index': self.a_create_index,
            'create_db': self.a_create_database,
            'use': self.a_use_database,
            'exit': self.a_exit,
            'show': self.a_show,
            'drop': self.a_drop,
            'search join': self.a_select_join,
        }

        self.a_load()

    def execute(self, statement):
            action = self.parser.p_parse(statement)
            if action:
                self.function[action['type']](action)

    def a_create(self, action):
        if self.currentDB is None:
            print("Did not Choose Database!")
            return
        try:
            if action['name'] in self.tables.keys():
                print("Table %s Already Exists!" % action['name'])
                return
            self.tables[action['name']] = Table(action['name'], action['cols'])
            if 'primary key' in action:
                self.tables[action['name']].primary = action['primary key']
                action_create_index = {'type': 'create_index', 'table': action['name'], 'name': 'primary key index', 'col': action['primary key']}
                self.tables[action['name']].t_create_index(action_create_index)
            if 'foreign key' in action:
                key_name, ref_table, ref_col = action['foreign key']
                if ref_table not in self.tables:
                    print("Foreign Table does not exist")
                    self.tables.pop(action['name'])
                    return
                if ref_col != self.tables[ref_table].primary:
                    print("This is not a primary key of target table")
                    self.tables.pop(action['name'])
                    return
                self.tables[action['name']].foreign = {
                    'name': key_name,
                    'ref_table': ref_table,
                    'ref_col': ref_col
                }
            self.a_update_table({
                'database': self.currentDB,
                'name': action['name']
            })
        except Exception as e:
            print(e.args[0])

    def a_create_index(self, action):
        # print(action)
        if self.currentDB is None:
            print("Did not Choose Database!")
            return
        if self.tables[action['table']].t_create_index(action):
            self.a_update_table({
                'database': self.currentDB,
                'name': action['table']
            })

    def a_insert(self, action):
        if self.currentDB is None:
            print("Did not Choose Database!")
            return
        if self.tables.get(action['table']) is None:
            print("ERROR！！！ No Table Named %s" % (action['table']))
        else:
            try:

                if self.tables[action['table']].foreign:
                    fore_name, ref_table, ref_name = self.tables[action['table']].foreign.values()
                    foreign_index = self.tables[action['table']].var.index(fore_name)
                    if int(action['values'][foreign_index]) not in self.tables[ref_table].data[ref_name]:
                        print("Invalid Foreign Key Value")
                        return
                self.tables[action['table']].t_insert(action)
                if action['table'] not in self.changed:
                    self.changed.append(action['table'])
                self.tables[action['table']].t_update_index()
            except Exception as e:
                print(e.args[0])

    def a_select(self, action):
        # print(action)
        if self.currentDB is None:
            print("Did not Choose Database!")
            return
        try:
            if action['table'] not in self.tables.keys():
                print('Error!!! No Table Named %s' % (action['table']))
                return
            res, type, orderby = self.tables[action['table']].t_select(action)
            if orderby:
                for key, value in res.items():
                    new_value = copy.deepcopy(value)
                    new_value = [i for _, i in sorted(zip(orderby, new_value))]
                    res[key] = new_value

            if action.get('limit'):
                try:
                    action['limit'] = int(action['limit'])
                    print_table(res, type, action['limit'])
                except Exception:
                    print('ERROR! Please Enter Integer As Limit Constraint!!')
                    return 
            else:
                print_table(res, type)
        except Exception as e:
            print(e.args[0])

    def a_select_join(self, action):
        if self.currentDB is None:
            print("Did not Choose Database!")
            return
        if action.get("conditions"):
            if len(action['conditions']) == 1:
                first_table = action['conditions'][0]['field'].split(".")[0]
                first_table_cond_field = action['conditions'][0]['field'].split(".")[1]
                first_table_col = action['join fields'][first_table]

                action_to_table1 = {
                    'type': 'search',
                    'table': first_table,
                    'conditions': [{
                        'field': first_table_cond_field,
                        'cond': action['conditions'][0]['cond'],
                    }]
                }
            elif len(action['conditions']) == 2:
                first_table = action['conditions'][0]['field'].split(".")[0]
                first_table_cond_field = action['conditions'][0]['field'].split(".")[1]
                first_table_cond_field2 = action['conditions'][1]['field'].split(".")[1]
                first_table_col = action['join fields'][first_table]

                action_to_table1 = {
                    'type': 'search',
                    'table': first_table,
                    'condition_logic': action['condition_logic'],
                    'conditions': [{
                        'field': first_table_cond_field,
                        'cond': action['conditions'][0]['cond'],
                    },
                        {
                            'field': first_table_cond_field2,
                            'cond': action['conditions'][1]['cond'],
                        }]
                }
            else:
                return
        else:
            first_table_tmp = list(action['join fields'].keys())[0]
            second_table_tmp = list(action['join fields'].keys())[1]
            if first_table_tmp not in self.tables or second_table_tmp not in self.tables:
                print("Tables not found")
                return
            lf = len(self.tables[first_table_tmp].data[action['join fields'][first_table_tmp]])

            ls = len(self.tables[second_table_tmp].data[action['join fields'][second_table_tmp]])

            if lf > ls:
                first_table = list(action['join fields'].keys())[0]
            else:
                first_table = list(action['join fields'].keys())[1]
                if action['operation'] == '<':
                    action['operation'] = '>'
                elif action['operation'] == '>':
                    action['operation'] = '<'
                elif action['operation'] == '>=':
                    action['operation'] = '<='
                elif action['operation'] == '<=':
                    action['operation'] = '>='
            first_table_col = action['join fields'][first_table]
            action_to_table1 = {
                'type': 'search',
                'table': first_table,
            }

        first_table_cols = self.tables[first_table].t_get_var()

        if action['fields'] == '*':
            first_table_fields = '*'
        else:
            first_table_fields = []
            for i in action['fields']:
                if i in first_table_cols:
                    first_table_fields.append(i)
                    if i != action['join fields'][first_table]:
                        action['fields'].remove(i)

        action_to_table1['fields'] = first_table_fields
        res1, type1, _ = self.tables[first_table].t_select(action_to_table1)

        for k, v in action['join fields'].items():
            if k != first_table:
                second_table_field = v
                second_table = k
        conditions = []
        seen_indices = set()
        second_table_tmp = list(action['join fields'].keys())[1]
        for index in self.tables[second_table_tmp].data[action['join fields'][second_table_tmp]]:
            if index not in seen_indices:
                condition = {
                    'field': second_table_field,
                    "cond": {
                        'operation': '=',
                        'value': f'{index}',
                    }
                }
                conditions.append(condition)
                seen_indices.add(index)

        action_to_table2 = {
            'type': 'search',
            'table': second_table,
            'condition_logic': 'OR',
            'fields': action['fields'],
            'conditions': conditions
        }

        res2, type2, _ = self.tables[second_table].t_select(action_to_table2)  # 到这儿都没问题
        types = {}
        types = merge_dict(types, type1, first_table)
        types = merge_dict(types, type2, second_table)
        l1 = len(res1[first_table_col])
        l2 = len(res2[second_table_field])
        if l1 * l2 < 10000:
            result = merge_data(res1, res2, first_table_col, second_table_field, first_table, second_table, action['operation'])
        else:
            if l1 * l2 < l1 * math.log10(l1) + l2 * math.log10(l2) + l1 + l2:
                result = merge_data(res1, res2, first_table_col, second_table_field, first_table, second_table, action['operation'])
            else:
                result = merge_sort_data(res1, res2, first_table_col, second_table_field, first_table, second_table, action['operation'])

        print_table(result, types)

    def a_delete(self, action):
        if self.currentDB is None:
            print("Did not Choose Database!")
            return
        if action['table'] not in self.tables.keys():
            print("No Table Named %s" % (action['table'].strip()))
            return
        # Foreign key constraint
        # 1. 检查其他table的外键属性索引用的table, 看下本table是否在里面, 在就检查, 不在就不检查
        # 2. 如果在 --> 返回Index --> 返回装了作为外键的set --> 如果要删的值是别人正在用的外键的值, 就不给改, 不执行delete
        # 3. 所以先在引用的表遍历一边这个column看有哪些值, 看是否在要删除的数据中, 是就终止操作
        foreign_table = ''
        foreign_col = ''
        for table in self.tables.keys():
            if len(self.tables[table].foreign.keys()) > 0:
                col, ref_table, ref_col = self.tables[table].foreign.values()
                if ref_table == action['table']:
                    foreign_table = table
                    foreign_col = col
        if foreign_table in self.tables.keys():
            primary = self.tables[action['table']].primary
            # Get data want to be deleted
            if action.get('condition_logic'):
                new_action = {'type': 'search', 'fields': [primary], 'table': action['table'],
                              'condition_logic': action.get('condition_logic'),
                              'conditions': action.get['conditions']}
            else:
                new_action = {'type': 'search', 'fields': [primary], 'table': action['table'],
                              'conditions': action['conditions']}
            res, type, orderby = self.tables[action['table']].t_select(new_action)
            delete_data = res[primary]
            for ref_value in self.tables[foreign_table].data[foreign_col]:
                if ref_value in delete_data:
                    print(f"Foreign Key Constraint:")
                    print(f"Data is used as foreign key value by {foreign_table}.{foreign_col}")
                    return
        # ---------------------------------
        self.tables[action['table']].t_delete(action)
        self.tables[action['table']].t_update_index_update()
        if action['table'] not in self.changed:
            self.changed.append(action['table'])

    def a_update(self, action):
        try:
            if action['table'] not in self.tables.keys():
                print(f"Not such table: {action['table']}")
                return

            if self.currentDB is None:
                print("Did not Choose Database!")
                return
            foreign_table = ''
            foreign_col = ''
            for table in self.tables.keys():
                if len(self.tables[table].foreign.keys()) > 0:
                    col, ref_table, ref_col = self.tables[table].foreign.values()
                    if ref_table == action['table']:
                        foreign_table = table
                        foreign_col = col
            if foreign_table in self.tables.keys():
                primary = self.tables[action['table']].primary
                # Get data want to be deleted
                if action.get('condition_logic'):
                    new_action = {'type': 'search', 'fields': [primary], 'table': action['table'],
                                  'condition_logic': action.get('condition_logic'),
                                  'conditions': action.get['conditions']}
                else:
                    new_action = {'type': 'search', 'fields': [primary], 'table': action['table'],
                                  'conditions': action['conditions']}
                res, type, orderby = self.tables[action['table']].t_select(new_action)
                update_data = res[primary]
                for ref_value in self.tables[foreign_table].data[foreign_col]:
                    # ref_value in update_data 意味着有在使用的主键要被修改了, 可以是[1, 2] 可以是 [3]
                    # ref_value == action['data'][primary] 意味着被使用的主键的值没有被修改
                    # ref_value != action['data'][primary] 意味着被使用的主键的值被修改了
                    if ref_value in update_data and ref_value != action['data'][primary]:
                        print(f"Foreign Key Constraint:")
                        print(f"Data is used as foreign key value by {foreign_table}.{foreign_col}")
                        return
            self.tables[action['table']].t_update(action)
            self.tables[action['table']].t_update_index_update()
            if action['table'] not in self.changed:
                self.changed.append(action['table'])
        except Exception as e:
            print(e.args[0])

    def a_create_database(self, action):
        if action['name'] not in self.database.keys():
            self.database[action['name']] = {}
            db_path = os.path.join('db', action['name'])
            if not os.path.exists(db_path):
                os.makedirs(db_path)
        else:
            print("Database '%s' Already Exists" % (action['name']))

    def a_use_database(self, action):
        if action['database'] in self.database.keys():
            self.currentDB = action['database']
            self.tables = self.database[action['database']]
        else:
            print("No Database Named %s" % (action['database']))

    def a_show(self, action):
        if action['kind'] == 'databases':
            databases = list(self.database.keys())
            print_table({
                'databases': databases
            })
        else:
            if self.currentDB is None:
                print("Did not Choose Database!")
                return
            tables = list(self.tables.keys())
            print_table({
                'tables': tables
            })

    def a_drop(self, action):
        if action['kind'] == 'database':
            if action['name'] not in self.database.keys():
                print("No Database Named %s", action['name'])
                return
            self.a_drop_db(action)
            del self.database[action['name']]
            if self.currentDB == action['name']:
                self.currentDB = None
        elif action['kind'] == 'table':
            if self.currentDB is None:
                print("Did not Choose Database!")
                return
            if action['name'] not in self.tables.keys():
                print("No Table Named %s", action['name'])
                return
            action['database'] = self.currentDB
            self.a_drop_table(action)
            del self.database[self.currentDB][action['name']]
            self.tables = self.database[self.currentDB]
        elif action['kind'] == 'index':
            if self.currentDB is None:
                print("Did not Choose Database!")
                return
            if action['table'] not in self.tables.keys():
                print("No Table Named %s", action['table'])
                return
            if self.tables[action['table']].t_drop_index(action):
                self.a_update_table({
                    'database': self.currentDB,
                    'name': action['table']
                })

    def a_drop_db(self, action):
        folder_path = os.path.join("db", action['name'])
        rmtree(folder_path)

    def a_drop_table(self, action):
        filepath = os.path.join("db", action['database'])
        filepath = os.path.join(filepath, action['name'])
        os.remove(filepath)

    def a_update_table(self, action):
        filepath = os.path.join("db", action['database'])
        filepath = os.path.join(filepath, action['name'])
        if os.path.exists(filepath):
            os.remove(filepath)
        f = open(filepath, 'wb')
        dump(self.tables[action['name']], f)
        f.close()

    def a_exit(self, action):
        self.a_save()
        os._exit(0)

    def a_load(self):
        db_path = os.path.join(os.getcwd(), "db")
        for path, db_list, _ in os.walk(db_path):
            for db_name in db_list:
                self.database[db_name] = {}
                for filepath, _, table_list in os.walk(os.path.join(path, db_name)):
                    for table_name in table_list:
                        f = open(os.path.join(filepath, table_name), 'rb')
                        self.database[db_name][table_name] = load(f)
                        f.close()

    def a_save(self):
        path = os.path.join(os.getcwd(), "db")
        f = None
        for db_name, tables in self.database.items():
            db_path = os.path.join(path, db_name)
            if not os.path.exists(db_path):
                os.makedirs(db_path)
            for table_name, table in tables.items():
                file_path = os.path.join(db_path, table_name)
                f = open(file_path, 'wb')
                dump(table, f)
                f.close()
