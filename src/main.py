from enum import Enum, unique


@unique
class MetaCommandResult(Enum):
    SUCCESS = 0
    UNCOGNIZED = 1


@unique
class StatementType(Enum):
    INSERT = 0
    SELECT = 1


class Statement:
    type: StatementType

    def __init__(self, command: str) -> None:
        if 'INSERT' == (command[:6]).upper():
            self.type = StatementType.INSERT
            return

        if 'SELECT' == command[:6].upper():
            self.type = StatementType.SELECT
            return

        raise Exception(f'Unrecognized statement: {command}')

    def execute(self) -> None:
        if type == StatementType.INSERT:
            print('This is where we would do an insert.')
        elif type == StatementType.SELECT:
            print('This is where we would do a select.')



def do_meta_command(command: str) -> MetaCommandResult:
    if '.EXIT' == command.upper():
        print('Bye~')
        exit(0)
    else:
        return MetaCommandResult.UNCOGNIZED
    

def main():
    while True:
        command = input('db > ')
        if command.startswith('.'):
            meta_command = do_meta_command(command)
            if meta_command == MetaCommandResult.SUCCESS:
                continue
            elif meta_command == MetaCommandResult.UNCOGNIZED:
                print(f'Unrecognized command: {command}')
                continue

        try:
            stat = Statement(command)
            stat.execute()
            print('Executed.')
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main()
