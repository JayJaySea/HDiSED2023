from flask import Flask
from misc import *
import sqlite3

app = Flask(__name__)

@app.route("/games/<offset>/<limit>", methods = ["GET"])
def games(offset, limit):
    cur = get_db_cursor()

    columns = table_columns(cur, "games")
    res = cur.execute("select * from games limit ? offset ?", (limit, offset))
    games = res.fetchall()

    cur.close()

    return to_objects(columns, games)

def get_db_cursor():
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")

    return cur
