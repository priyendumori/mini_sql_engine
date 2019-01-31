import sys, re, os
sys.path.insert(0,os.getcwd() + "/sqlparse-0.2.4")
import sqlparse
import csv

table_info={}
query_cols = []
nat_join = []
result = []

'''
    LOAD MetaData from metadata.txt file
    to table_info dictionary
    eg, {'table1':['C1','C2']}
'''
def load_meta_data():
    try:
        with open('metadata.txt',"r") as f:
            table=""
            for l in f:
                stripped_line = l.strip()
                if stripped_line == '<begin_table>':
                    attributes = []
                    read_cols = True
                elif stripped_line == '<end_table>':
                    table_info[table] = attributes
                elif read_cols:
                    table = stripped_line
                    read_cols = False
                else:
                    attributes.append(stripped_line)
    except:
        print "Matadata file missing or corrupted"
        sys.exit()
        
'''
    get required columns
    call proper aggregate function according to aggr_list
'''
def aggregate(column,aggr_list):
    result = ""
    for i in range(len(column)):
        index = pick_columns([column[i]])
        if aggr_list[i].upper() == "MAX":    
            result+=str(get_max(index))+"\t"

        elif aggr_list[i].upper() == "MIN":
            result+=str(get_min(index))+"\t"

        elif aggr_list[i].upper() == "SUM":
            result+=str(get_sum(index))+"\t"

        elif aggr_list[i].lower() == "avg":
            result+=str(get_avg(index))+"\t"
    return result

'''
    get max of a column
'''
def get_max(index):
    try:
        max_elem = result[0][index[0]]
        for row in result:
            if max_elem < row[index[0]]:
                max_elem = row[index[0]]
    except:
        max_elem = 'null'
    return max_elem

'''
    get min of a column
'''
def get_min(index):
    try:
        min_elem = result[0][index[0]]
        for row in result:
            if min_elem > row[index[0]]:
                min_elem = row[index[0]]
    except:
        min_elem = 'null'
    return min_elem

'''
    get sum of a column
'''
def get_sum(index):
    try:
        total = 0
        for row in result:
            total += row[index[0]]
    except:
        total = 'null'
    return total

'''
    get avg of a column
'''
def get_avg(index):
    try:
        total = get_sum(index)
        avg = float(total)/float(len(result))
    except:
        avg = 'null'
    return avg
        
'''
    checks for error in query formation
'''
def error_checks(distinct_flag, where_flag, where_condition,components):
    if(distinct_flag>1):
        print "SYNTAX ERROR in the usage of distinct keyword"
        sys.exit()

    if where_flag and len(where_condition.strip())==0:
        print "SYNTAX ERROR in where clause"
        sys.exit()

    if len(components)> 5 and not where_flag :
        print "SYNTAX ERROR"
        sys.exit()

    if len(components)== 5:
        if not where_flag and distinct_flag==0:
            print "SYNTAX ERROR"
            sys.exit()

'''
    select only the columns required in the query
    and remove duplicate column in case of natural join
'''
def pick_columns(column):

    if len(query_cols) == 0:
        return []

    cols_in_result = []

    if ''.join(column) == '*':
        column = query_cols
    for col in column:
        try:
            cols_in_result.append(query_cols.index(col))
        except ValueError:
            flag = False
            c = ""
            for i in query_cols:
                if i.endswith("."+col):
                    flag = True
                    c = i

            if(flag == 1):
                index = query_cols.index(c)
                cols_in_result.append(index)
            else:
                return []

    if(len(nat_join)>0):
        for repeated in nat_join:
            if repeated[0] in cols_in_result and repeated[1] in cols_in_result:
                index1 = cols_in_result.index(repeated[0])
                index2 = cols_in_result.index(repeated[1])
                if index1<index2:
                    del cols_in_result[index2]
                else:
                    del cols_in_result[index1]

    if(len(cols_in_result) == 0):
        print "SYNTAX ERROR in columns"
        sys.exit()

    return cols_in_result


'''
    Remove duplicates from a column in case 
    distinct is required in query
'''
def unique_ans(result):
    try:
        row = result.split('\n')
        unique = []
        for r in row:
            if r not in unique:
                unique.append(r)
        unique_col = '\n'.join(unique)
    except Exception:
        print "SYNTAX ERROR"
        sys.exit()
    return unique_col

        
'''
    populate list of aggregate functions aggr_list and
    list of columns needed col_list
'''
def get_cols_and_aggregate(cols,col_list,aggr_list):
    cols = map(str.strip,cols)
    error_code = False
    for col in cols:
        if col.upper().startswith("AVG") or \
            col.upper().startswith("MIN") or \
            col.upper().startswith("SUM") or \
            col.upper().startswith("MAX") :
            
            aggr_list.append(col.split("(")[0])
            col_list.append(col[4:col.index(")")])
            error_code = True
        else:
            if error_code:
                return True
            else:
                col_list.append(col)
    return False

'''
    read rows from from table and store them in result as list of lists
    open csv also check for int in quotes('' and "")
    Also if distinct is needed, don't append duplicate rows
'''
def read_table(table_name,distinct):
    table_name = table_name + ".csv"
    result = []
    try:
        filereader = csv.reader(open(table_name),delimiter=',')
    except:
        print "Query is improperly formed."
        sys.exit()
        
    for line in filereader:
        for i in range(len(line)):
            if line[i][-1] == "\'" or line[i][-1] == '\"':
                line[i] = line[i][1:-1]
        line = map(int,line)

        if distinct == 1:
            if line not in result:
                result.append(line)
        else:
            result.append(line)

    return result


'''
    doing cartesian product of two tables and 
    return them as list of lists
'''
def cartesian_product(table0, table1):
    t = []
    for t0_row in table0:
        for t1_row in table1:
            t.append(t0_row + t1_row)
    return t

'''
    if only one table is present, read that and return
    if more than one table is present then read them and take their cartesian product
'''
def join_tables(table_names,distinct_flag):
    if len(table_names) == 1:
        return read_table(table_names[0],distinct_flag)
    
    table0 = read_table(table_names[0],distinct_flag)
    for i in range(1,len(table_names)):
        table1 = read_table(table_names[i],distinct_flag)
        table0 = cartesian_product(table0,table1)
        
    return table0


'''
    for all the tables required in querystring,
    populate cols_in_query as tablename.columnname
    eg, ['table1.C1','table1.C2','table2.C1']
'''
def get_cols_in_query(table_names):
    cols_in_query = []

    for table_name in table_names:
        if table_name not in table_info:
            return []
        else:
            schema = table_info[table_name]
            for attributes in schema:
                cols_in_query.append(table_name+"."+attributes)
                
    return cols_in_query


'''
    identifying the relational operator in where condition
'''
def get_operator(con):
    op = ['>','<','=']
    dop = ['>=','<=']
    index = -1
    for i in xrange(len(con)):
        if con[i] in op:
            index=i
            break
    if index==-1:
        return ""
    operator = con[index]+con[index+1]
    if operator in dop:
        return operator
    return con[index]

'''
    for a condition con,
    get condition splitted in separate as
    [operand1, operand2, operator]
'''
def get_operands(con):
    separate = []
    try:
        operator = get_operator(con)
        separate = con.split(operator)
        separate = map(str.strip,separate)
        if operator == "=":
            separate.append("==")
        else:
            separate.append(operator)            

    except:
        print "SYNTAX ERROR occurred in where condition"
    
    return separate

'''
    find column number corresponding to col
    using already populated query_cols
'''
def get_col_number(col):
    col_number = -1
    index = 0

    for c in query_cols:
        if c.endswith("."+col) or c.upper() == col.upper():
            col_number = index
        index+=1
        
    return col_number

def evaluate_where_condition(where_condition):
    try:
        conds = where_condition.split(" ")
        conds = map(str.strip,conds)
        connector = []
        for cond in conds:
            if cond.upper().strip() == "AND" or cond.upper().strip() == "OR":
                connector.append(cond)

        connector = map(str.lower,connector)
        delimiters = "and","or"
        regexPattern = '|'.join(map(re.escape, delimiters))+"(?i)"
        con = re.split(regexPattern, where_condition)
        con = map(str.strip,con)

        for i in range(len(con)) :

            split = get_operands(con[i])
            split = map(str.strip,split)

            left = get_col_number(split[0].strip())
            right = get_col_number(split[1].strip())

            if left >-1 and right >-1:
                split[0] = split[0].replace(split[0],"result[i]["+str(left)+"]")
                split[1] = split[1].replace(split[1],"result[i]["+str(right)+"]")

            elif left>-1:
                split[0] = split[0].replace(split[0],"result[i]["+str(left)+"]")

            else:
                print "SYNTAX ERROR"
                sys.exit()

            tup = split[0],split[1]
            con[i] = split[2].join(tup)

        new_con = con[0]+" "

        counter = 0
        for j in xrange(1,len(con)):
            new_con+= connector[counter].lower()+" "
            new_con+=con[j]+" "
            counter+=1

        temp_res = []
        for i in range(len(result)):
            if eval(new_con):
                temp_res.append(result[i])

    except Exception:
        print "SYNTAX ERROR"
        sys.exit()

    return temp_res

'''
    if two cols in where condition are being operated by ==
    only one of them has to be printed 
    so a tuple containing both their column numbers is appended to nat_join
'''
def natural_join(where_condition):
    global nat_join
    try:
        dot = '.'
        delimiters="and","or"
        regexPattern = '|'.join(map(re.escape, delimiters))+"(?i)"
        c = re.split(regexPattern, where_condition)
        c = map(str.strip,c)

        for i in range(len(c)):
            exp = get_operands(c[i])
            exp = map(str.strip,exp)

            if dot in exp[0] and dot in exp[1]:
                if exp[2].strip() == "==":
                    col1 = get_col_number(exp[0].strip())
                    col2 = get_col_number(exp[1].strip())
                    repeated = col1,col2
                    nat_join.append(repeated)

    except Exception as e:
        print "SYNTAX ERROR"
        sys.exit()



def process(query_string):
    global query_cols,result

    parsed_query = sqlparse.parse(query_string)[0].tokens
    query_type = sqlparse.sql.Statement(parsed_query).get_type()

    if query_type.upper() != 'SELECT':
        print "Query is NOT VALID"
        sys.exit()

    components = []
    parts = sqlparse.sql.IdentifierList(parsed_query).get_identifiers()
    for part in parts:
        components.append(str(part))

    table_names = ""
    cols_in_query = ""
    where_condition = ""
    distinct_flag = 0
    where_flag = False

    for i in xrange(0,len(components)):
        if components[i].upper() == "DISTINCT":
            distinct_flag+=1
        elif components[i].upper() == "FROM":
            table_names = components[i+1]
        elif components[i].upper().startswith("WHERE"):
            where_flag=True
            where_condition = components[i][6:].strip()

    error_checks(distinct_flag, where_flag, where_condition,components)

    if distinct_flag == 1:
        cols_in_query = components[2]
    else:
        cols_in_query = components[1]

    column = []
    aggr = []
    
    if get_cols_and_aggregate(cols_in_query.split(","),column,aggr):
        print "SYNTAX ERROR - aggregate columns and normal columns cannot be used together"
        sys.exit()

    table_names = table_names.split(",")
    table_names = map(str.strip,table_names)
    query_cols = get_cols_in_query(table_names)

    result = []
    col_header = ""

    result = join_tables(table_names,distinct_flag)

    # If there is a where condition
    if where_condition != "":
        result = evaluate_where_condition(where_condition)
        # If there are aggregate functions with where condition
        if len(aggr)>0:
            co = pick_columns(column)
            for i in range(len(co)):
                col_header+=aggr[i]+"("+query_cols[co[i]]+"),"
            col_header = col_header[:-1]
            col_header = col_header+'\n'
        natural_join(where_condition)


    final_result = ""

    #If aggregate functions not present
    if len(aggr) == 0:
        res_col = pick_columns(column)
        if len(res_col) == 0:
            print "SYNTAX ERROR - result is null "
            sys.exit()
        col_header = []
        for i in res_col:
            col_header.append(query_cols[i])
        col_header = ",".join(col_header)
        col_header += '\n'

        for i in range(len(result)):
            for j in range(len(res_col)):
                final_result+=str(result[i][res_col[j]])+"\t"

            final_result+='\n'

    #If aggregate functions present
    else:
        try:
            col_header = ""
            co = pick_columns(column)
            for i in range(len(co)):
                col_header+=aggr[i]+"("+query_cols[co[i]]+"),"
            col_header = col_header[:-1]
            col_header = col_header+'\n'

            if len(col_header)>0:
                final_result+=aggregate(column,aggr)
            else:
                final_result = 'null'
        except IndexError as e:
            print "SYNTAX ERROR"

    if distinct_flag == 1:
        final_result = unique_ans(final_result)

    if final_result == "":
        print "EMPTY"
    else:
        print col_header+final_result

def main():
    load_meta_data()
    query_string = sys.argv[1].split(";")[0].strip()
    process(query_string)

if __name__ == "__main__":
    main()
