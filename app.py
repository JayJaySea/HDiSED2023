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

@app.route("/players/<offset>/<limit>", methods = ["GET"])
def players(offset, limit):
    cur = get_db_cursor()

    columns = table_columns(cur, "players")
    res = cur.execute("select * from players limit ? offset ?", (limit, offset))
    players = res.fetchall()

    cur.close()

    return to_objects(columns, players)

@app.route("/player/<id>", methods = ["GET"])
def player_by_id(id: str):
    cur = get_db_cursor()
    print(id)

    columns = table_columns(cur, "players")
    res = cur.execute("select * from players where id = ?", (id,))
    player = res.fetchall()

    cur.close()

    return to_objects(columns, player)

@app.route("/tournaments/<offset>/<limit>", methods = ["GET"])
def tournaments(offset, limit):
    cur = get_db_cursor()

    columns = table_columns(cur, "tournaments")
    res = cur.execute("select * from tournaments limit ? offset ?", (limit, offset))
    tournaments = res.fetchall()

    cur.close()

    return to_objects(columns, tournaments)


@app.route("/tournament/<id>", methods = ["GET"])
def tournament_by_id(id: str):
    cur = get_db_cursor()
    print(id)

    columns = table_columns(cur, "tournaments")
    res = cur.execute("select * from tournaments where id = ?", (id,))
    tournament = res.fetchall()

    cur.close()

    return to_objects(columns, tournament)


def get_db_cursor():
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")

    return cur
