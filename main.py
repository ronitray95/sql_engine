#!/usr/bin/env python3

import csv
import sqlparse
import sys
import copy
from models import *
from sqlparse.sql import *
from sqlparse.tokens import *
from sqlparse import *
import re
tableList = {}
tableListRows = {}  # map of table rows
columnData = {}
keywords = ['select', 'distinct', 'from', 'where', 'group', '', '', '', '', ]
functions = ['sum', 'count', 'max', 'min', 'avg']


def getSQL():
    if(len(sys.argv) <= 1):
        print('Insufficient parameters')
        return
    query = ' '.join(sys.argv[1:])
    query = query.lower()
    print(query)
    parsed = sqlparse.parse(query)
    tokens = parsed[0].tokens
    isDistinct = False
    for t in tokens:
        if str(t).lower() == 'distinct':
            isDistinct = True
        print(t)
    opTables = (8 if isDistinct else 6)
    opTables = str(tokens[opTables]).split(',')
    opCols = 4 if isDistinct else 2
    opCols = str(tokens[opCols]).split(',')
    aggCols = {}
    for col in opCols:
        if '(' not in col:
            continue
        agg = re.findall('[^( )]+', col)[0]
        cc = re.findall('[^( )]+', col)[1]
        opCols.remove(col)
        aggCols[cc] = agg
    try:
        where = next(token for token in tokens if isinstance(token, Where))
        # print(where)
        # next(token for token in where.tokens if isinstance(token, Comparison))
        condition = (str(where).strip()).lower().split(' ')[1:]
        condition = ''.join(condition)
        condition = condition.lower()
    except Exception:
        condition = None
    cond1 = None
    cond2 = None
    condType = None
    if 'and' in str(condition).lower():
        cond1 = str(condition).lower().split('and')[0]
        cond2 = str(condition).lower().split('and')[1]
        cond1 = str(cond1)
        cond2 = str(cond2)
        condType = 'and'
    elif 'or' in str(condition).lower():
        cond1 = str(condition).lower().split('or')[0]
        cond2 = str(condition).lower().split('or')[1]
        cond1 = str(cond1)
        cond2 = str(cond2)
        condType = 'or'
    else:
        cond1 = str(condition)

    # print(condition)
    # print(opCols)
    # print(opTables)
    allColumns = []
    allColumns = tableListRows[opTables[0]]
    if len(opTables) > 1:
        for tt in opTables[1:]:
            temp = []  # copy.deepcopy(allColumns)
            for t in allColumns:
                for x in tableListRows[tt]:
                    temp.append(t+x)
            allColumns = copy.deepcopy(temp)
    allColNames = []
    for tt in opTables:
        for c in tableList[tt].column:
            allColNames.append(c)
    allColumns.insert(0, allColNames)
    # print(len(allColumns))
    # for c in allColumns:
    #     print(c)
    # start where evaluation
    if condition is not None:
        if cond2 is None:
            cond1 = cond1.replace(';', '')
            x1 = re.findall('^[A-Za-z0-9]+', cond1)[0]
            x2 = re.findall('[A-Za-z0-9]+$', cond1)[0]
            cname1, cname2 = x1, x2
            op = re.findall('[<=>=!]+', cond1)[0]
            print(cname1, cname2, op)
            ic1 = (allColumns[0]).index(cname1)
            print(ic1)
            # ic2 = 0
            # try:
            #     ic2 = int(cname2)
            # except Exception:
            #     ic2 = (allColumns[0]).index(cname2)
            for l in allColumns[1:]:
                if not checkCondition(l[ic1], l, cname2, op, allColumns):
                    l[0] = None
        else:
            cond1 = cond1.replace(';', '')
            cond2 = cond2.replace(';', '')
            cname11 = re.findall('^[A-Za-z0-9]+', cond1)[0]
            cname12 = re.findall('[A-Za-z0-9]+$', cond1)[0]
            cname21 = re.findall('^[A-Za-z0-9]+', cond2)[0]
            cname22 = re.findall('[A-Za-z0-9]+$', cond2)[0]
            # cname11, cname12 = re.findall('[^<=>=!]', cond1)[0:2]
            # cname21, cname22 = re.findall('[^<=>=!]', cond2)[0:2]
            op1 = re.findall('[<=>=!]', cond1)[0]
            op2 = re.findall('[<=>=!]', cond2)[0]
            # print(cname1,cname2,op)
            ic11 = (allColumns[0]).index(cname11)
            ic21 = (allColumns[0]).index(cname21)
            for l in allColumns[1:]:
                vv = False
                if condType == 'and':
                    vv = checkCondition(l[ic11], l, cname12, op1, allColumns) and checkCondition(
                        l[ic21], l, cname22, op2, allColumns)
                else:
                    vv = checkCondition(l[ic11], l, cname12, op1, allColumns) or checkCondition(
                        l[ic21], l, cname22, op2, allColumns)
                if not vv:
                    l[0] = None

    # finished where evaluation
    allColumns = [i for i in allColumns if i.count(None) == 0]
    # start group by evaluation
    groupByCol = 0
    for t in tokens:
        if str(t) == 'group by':
            groupByCol = groupByCol+2
            break
        groupByCol += 1
    if groupByCol >= len(tokens):
        groupByCol = None
    else:
        groupByCol = str(tokens[groupByCol]).strip().lower()
    # print(groupByCol)

    if groupByCol is not None:
        grpIndex = (allColumns[0]).index(groupByCol)
        allColumns = sorted(allColumns[1:], key=lambda x: x[(
            allColumns[0]).index(groupByCol)], reverse=False)

        startCol = 0
        while startCol < len(allColNames):
            if startCol == grpIndex:
                startCol += 1
                continue
            start = 0
            end = 0
            while end < len(allColumns):
                initVal = allColumns[start][grpIndex]
                newlist = []
                # print(start,grpIndex)
                while end < len(allColumns) and initVal == allColumns[end][grpIndex]:
                    newlist.append(allColumns[end][startCol])
                    allColumns[end][startCol] = None
                    end += 1
                allColumns[start][startCol] = newlist
                start = end
            startCol += 1
        allColumns = [i for i in allColumns if i.count(None) == 0]
        allColumns.insert(0, allColNames)

    # end group by evaluation

    # start select evaluation

    # find count(*)
    if '*' in aggCols.keys() and aggCols['*'] == 'count':
        print('count(*)\n', (len(allColumns)-1))
        return None

    if not (len(opCols) == 1 and opCols[0] == '*'):
        i = 0
        delCols = 0
        # print(allColumns[0])
        for col in allColumns[0]:
            if col not in opCols and col not in aggCols:
                allColumns[0][i] = None
                delCols += 1
            i += 1
        i = 0

        for i in range(delCols):
            index = -1
            for col in range(len(allColumns[0])):
                if allColumns[0][col] is None:
                    index = col
                    break
            if index != -1:
                for col in allColumns:
                    del(col[index])

    if groupByCol is not None:
        for j in range(len(allColumns[0])):
            for i in range(1, len(allColumns)):
                if allColumns[0][j] in aggCols:
                    allColumns[i][j] = aggregate(
                        allColumns[i][j], aggCols[allColumns[0][j]])
                elif isinstance(allColumns[i][j], list):
                    allColumns[i][j] = allColumns[i][j][0]
    # end select evaluation

    # start distinct evaluation
    if isDistinct:
        newlist = []
        for col in allColumns:
            if col not in newlist:
                newlist.append(col)
        allColumns = newlist
    # end distinct evaluation

    # start order by evaluaion
    orderByCol = 0
    orderByAsc = False
    for t in tokens:
        if str(t) == 'order by':
            orderByCol = orderByCol+2
            break
        orderByCol += 1
    if orderByCol >= len(tokens):
        orderByCol = None
    else:
        orderByCol = str(tokens[orderByCol]).strip().lower()
        if 'desc' in orderByCol:
            orderByAsc = True
        orderByCol = orderByCol.split()[0]

    if orderByCol is not None and orderByCol in allColumns[0]:
        allColNames = allColumns[0]
        allColumns = sorted(allColumns[1:], key=lambda x: x[(
            allColumns[0]).index(orderByCol)], reverse=orderByAsc)
        allColumns.insert(0, allColNames)

    # end order by evaluaion
    for c in allColumns:
        print(c)


def aggregate(ll, aggc):
    if aggc == 'max':
        return max(ll)
    elif aggc == 'min':
        return min(ll)
    elif aggc == 'sum':
        return sum(ll)
    elif aggc == 'count':
        return len(ll)
    elif aggc == 'avg':
        return sum(ll) / len(ll)
    return ll[0]


def checkCondition(no1, ll, id, op, allColumns):
    no2 = 0
    try:
        x = int(id)
        no2 = x
    except Exception:
        no2 = ll[(allColumns[0]).index(id)]
    # print('for',no1,no2,op)
    if op == '=':
        return no1 == no2
    if op == '<=':
        return no1 <= no2
    if op == '>=':
        return no1 >= no2
    if op == '<':
        return no1 < no2
    if op == '>':
        return no1 > no2


def readTables():
    #t = Table('t1')
    ll = []
    f = open('metadata.txt').readlines()
    # print(f)
    for line in f:
        line = line.strip()
        if line == '<begin_table>':
            continue
        if line == '<end_table>':
            if(len(ll) <= 1):
                print('Skipping one table. No info in metadata')
                continue
            columns = ll[1:]
            tableList[ll[0]] = Table(ll[0], columns)
            ll = []
            continue
        ll.append(line.lower())
    for t in tableList.values():
        print(f'{t.name}', f'{t.column}')
    # print(tableList)


def readTableData():
    for table in tableList.values():
        with open(table.name+'.csv') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            columnNames = table.column
            for row in csv_reader:

                if table.name not in tableListRows:
                    tableListRows[table.name] = []
                rowa = list(map(int, row))
                tableListRows[table.name].append(rowa)

                for i in range(len(row)):
                    if table.column[i] not in columnData:
                        cc = Column(table.column[i])
                        num = row[i].replace('\'', '')
                        num = num.replace('\"', '')
                        try:
                            num = int(num)
                            cc.addData(num)
                            columnData[table.column[i]] = cc
                        except ValueError:
                            print(f'Not of type integer {num}')
                    else:
                        cc = columnData[table.column[i]]
                        num = row[i].replace('\'', '')
                        num = num.replace('\"', '')
                        try:
                            num = int(num)
                            cc.addData(num)
                            columnData[table.column[i]] = cc
                        except ValueError:
                            print(f'Not of type integer {num}')
                line_count += 1
            print(f'Processed {line_count} lines for table {table.name}')

    # for table in tableList.values():
    #     for cols in table.column:
    #         print(f'Column {cols}', columnData[cols].data)
    # for x in tableListRows.keys():
    # 	print(tableListRows[x])


def main():
    readTables()
    readTableData()
    getSQL()


if __name__ == "__main__":
    main()
