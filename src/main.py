from enum import Enum, unique
from typing import List

from parse import parse


@unique
class MetaCommandResult(Enum):
    SUCCESS = 0
    UNCOGNIZED = 1


@unique
class StatementType(Enum):
    INSERT = 0
    SELECT = 1


class Row:
    id: int
    username: str
    email: str

    def __init__(self, id: int, username: str, email: str) -> None:
        self.id = id
        self.username = username
        self.email = email
    

class Table:
    row_num: int
    pages: List[Row]

    def __init__(self) -> None:
        self.row_num = 0
        self.pages = []


class Statement:
    type: StatementType
    row_to_insert: Row
    table: Table

    def __init__(self, command: str, table: Table) -> None:
        self.table = table
        upper_command = command.upper()
        if 'INSERT' == upper_command[:6]:
            self.type = StatementType.INSERT
            result = parse('INSERT {id:d} {username} {email}', upper_command)
            if result is None or len(result.spans) < 3:
                raise Exception('Syntax error. Could not parse statement.')
            
            self.row_to_insert = Row(result['id'], result['username'], result['email'])
            return

        if 'SELECT' == upper_command[:6]:
            self.type = StatementType.SELECT
            return

        raise Exception(f'Unrecognized statement: {command}')

    def execute(self) -> None:
        if self.type == StatementType.INSERT:
            self.__insert()
        elif self.type == StatementType.SELECT:
            self.__select()

    def __insert(self) -> int:
        row_num = self.table.row_num
        self.table.pages.append(self.row_to_insert)
        self.table.row_num += 1
        return self.table.row_num - row_num

    def __select(self) -> List[Row]:
        for row in self.table.pages:
            print(f'({row.id}, {row.username}, {row.email})')


def do_meta_command(command: str) -> MetaCommandResult:
    if '.EXIT' == command.upper():
        print('Bye~')
        exit(0)
    else:
        return MetaCommandResult.UNCOGNIZED
    

def main():
    table = Table()
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
            stat = Statement(command, table)
            stat.execute()
            print('Executed.')
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main()
