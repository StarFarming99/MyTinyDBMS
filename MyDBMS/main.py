import cmd
import time
import api


class Main(cmd.Cmd):
    print('''
          ┌─┐       ┌─┐ + +
       ┌──┘ ┴───────┘ ┴──┐++
       │                 │
       │       ───       │++ + + +
       ███████───███████ │+         ,,,*/%@@@@@&@@@&(,.,,,,/%@@@@&&@@@%*.,,,/#@@@%,,,,,,,,,,,(@@@#/,,./@@%#&@&&#.,,
       │                 │+         ,,,,,,@@@,,,,,,,%@@%.,,,.&@&,,,,.,/@@(,,,,,@@@@,,,,,,,,,&&@@/,,,.@@,,.,,,,&(,,,
       │       ─┴─       │          ,,,,,,@@@,,,,,,,,*@@@,,,,&@&,,,,,,*@@#,,,,,@.&@@,,,,,,,&%*@@/,,,,&@@%,,,,,,/,,,
       │                 │          ,,,,,,@@@,,,,,,,,,@@@*,,,&@@@@@@@@@#,,,,,,,@,.&@@,,,,,@(.,@@/,,,,,.%@@@@&,,,.,,
       └───┐         ┌───┘          ,,,,,,@@@,,,,,,,,.@@@,,,,&@&,,,,,.(@@@.,,,,@,,,#@@*,,@*,,*@@/,,,,,.,,,.#@@@@,,,  
           │         │              ,,,,,,@@@,,,,,,,.%@@*,,,,&@&,,,,,,,#@@(,,,,@,,,,/@@#@*,,,*@@/,,,,@.,,,,,,,@@%,,   
           │         │   + +        ,,,,,,@@@,,,,,/@@@*,,,,,,@@&,,,,,*@@@/,,,,,@,,,,,*@@,,,,,*@@/,,,,@@(.,,,,/@&.,,   
           │         │                      
           │         └──────────────┐   
           │                        │   
           │                        ├─┐ 
           │                        ┌─┘ 
           │                        │
           └─┐  ┐  ┌───────┬──┐  ┌──┘  + + + +
             │ ─┤ ─┤       │ ─┤ ─┤
             └──┴──┘       └──┴──┘  + + + +
             ''')

    def __init__(self):
        super().__init__()
        self.instance = api.API()

    def default(self, line: str) -> None:
        try:
            time_start = time.time()
            self.instance.execute(line)
            time_end = time.time()
            print(" time elapsed : %fs." % (time_end - time_start))
        except Exception as e:
            print(e.args[0])
    def do_commit(self, args):
        try:
            time_start = time.time()
            for name in self.instance.changed:
                self.instance.a_update_table({
                    'database': self.instance.currentDB,
                    'name': name
                })
                for table in self.instance.tables.values():
                    for col in table.statistics.keys():
                        # if not table.statistics[col].isdigit():
                        #     continue
                        table.statistics[col]['MIN'] = min(table.data[col])
                        table.statistics[col]['MAX'] = max(table.data[col])

            time_end = time.time()
            print(" time elapsed : %fs." % (time_end - time_start))
            print('Modifications has been commited to local files')
            self.instance.changed = []
        except Exception as e:
            print(e.args[0])

    def do_exit(self, args):
        print("Goodbye!")
        return True


if __name__ == '__main__':
    Main.prompt = 'SQL >'
    Main().cmdloop()