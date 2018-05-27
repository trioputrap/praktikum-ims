import pymysql
import datetime
import json
import config

dml_mode = {"INSERT" : 1, "UPDATE" : 2, "DELETE" : 3}

def str_dtime(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

def get_row_json(data, mode, id=None):
    row = {
        "datetime" : str(datetime.datetime.now()),
        "dml_mode" : dml_mode[mode],
        "data": data
    }
    if(not id is None ) : row['id'] = id
    return row

def save_json(file, row):
    try:
        f = open(file, "r")
        data = json.loads(f.read())
        f.close()
    except:
        data = []
    print(data)
    data.append(row)
    f = open(file, "w")
    f.write(json.dumps(data, default=str_dtime, indent=4))
    f.close()

def read_json(file):
    try:
        f = open(file, "r")
        data = json.loads(f.read())
        f.close()
    except:
        data = []

    return data

def save_data(tb_name, data, cursor):
    cursor.execute("TRUNCATE "+tb_name)
    for row in data:
        query = query_insert_builder(tb_name, row)
        cursor.execute(query)

def find_by_id(data, id):
    i = 0
    for row in data:
        if (row['id'] == id):
            return i
        i+=1
    return -1

def query_update_builder(table, data, id):
    sql = "UPDATE " + table['name'] + " SET "
    first = True
    for field, value in data.items():
        sql+="," if not first else ""
        sql+="`"+field+"`="
        sql += "NULL" if value is None else "'" + str(value) + "'"
        first = False
    sql += " WHERE `"+ table['id'] +"` = '" + str(id) + "'"
    return sql

def query_insert_builder(tb_name, data):
    sql = "INSERT INTO " + tb_name +"("
    first=True
    for field in data:
        sql+="," if not first else ""
        sql+=field
        first=False
    sql+=") VALUES("
    first = True
    for value in data.values():
        sql+="," if not first else ""
        sql += "NULL" if value is None else "'" + str(value) +"'"
        first=False
    sql += ")"
    return sql

def query_delete_builder(table, id):
    sql = "DELETE FROM " + table['name']
    sql += " WHERE `"+ table['id'] +"` = '" + str(id) + "'"
    return sql

def connect(db_config):
    return pymysql.connect(
        db_config['host'],
        db_config['user'],
        db_config['pass'],
        db_config['db_name']
    )

def get_cursor(db):
    return db.cursor(pymysql.cursors.DictCursor)

def print_timestamp(msg):
    print("\n[" + str(datetime.datetime.now()) + "]")
    print("> " + msg)

def get_modified_col(row_main, row_history):
    row_update = {}
    for col, val in row_history.items():
        if (row_main[col] != val):
            row_update[col] = row_main[col]

    return row_update
