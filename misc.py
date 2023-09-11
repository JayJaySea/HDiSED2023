from flask import Flask
import sqlite3
app = Flask(__name__)

def table_columns(cur, table_name):
    sql = "select * from %s where 1=0;" % table_name
    cur.execute(sql)
    return [d[0] for d in cur.description]

def to_objects(columns, records):
    objects = []

    for record in records:
        objects.append({columns[i]: record[i] for i in range(len(columns))})

    return objects

def get_db_cursor():
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")

    return cur
