#!/usr/bin/env python3

class Table:
    def __init__(self, name):
        self.name = name
        self.column = []

    def __init__(self, name, column):
        self.name = name
        self.column = column

    def setName(self, name):
        self.name = name

    def addColumn(self, column):
        self.column.append(column)


class Column:
    def __init__(self, table, name):
        self.name = name
        self.tableName = table
        self.data = []

    def addData(self, data: int):
        self.data.append(data)
