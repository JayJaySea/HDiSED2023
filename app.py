from lichess_api import *
from misc import *


@app.route("/games/<offset>/<limit>", methods = ["GET"])
def games(offset, limit):
    cur = get_db_cursor()

    columns = table_columns(cur, "games")
    res = cur.execute("select * from games limit ? offset ?", (limit, offset))
    games = res.fetchall()

    cur.close()

    return to_objects(columns, games)

@app.route("/games/player/<id>", methods = ["GET"])
def games_by_player(id):
    cur = get_db_cursor()

    columns = table_columns(cur, "games")
    res = cur.execute("select * from games where white_id = ? or black_id = ?", (id, id))
    games = res.fetchall()

    cur.close()

    return to_objects(columns, games)

@app.route("/games/tournament/<id>", methods = ["GET"])
def games_by_tournament(id):
    cur = get_db_cursor()

    columns = table_columns(cur, "games")
    res = cur.execute("select * from games where tournament_id = ?", (id,))
    games = res.fetchall()

    cur.close()

    return to_objects(columns, games)

@app.route("/game/<id>", methods = ["GET"])
def game_by_id(id: str):
    cur = get_db_cursor()

    columns = table_columns(cur, "games")
    res = cur.execute("select * from games where id = ?", (id,))
    game = res.fetchall()

    cur.close()

    return to_objects(columns, game)

@app.route("/players/<offset>/<limit>", methods = ["GET"])
def players(offset, limit):
    cur = get_db_cursor()

    columns = table_columns(cur, "players")
    res = cur.execute("select * from players limit ? offset ?", (limit, offset))
    players = res.fetchall()

    cur.close()

    return to_objects(columns, players)

@app.route("/players/tournament/<id>", methods = ["GET"])
def players_by_tournament(id):
    cur = get_db_cursor()

    query = """
select distinct white_id from games where tournament_id = ?
UNION
select distinct black_id from games where tournament_id = ?
"""
    res = cur.execute(query, (id, id))
    player_ids = res.fetchall()
    player_ids = [t[0] for t in player_ids]

    placeholders = ', '.join(['?'] * len(player_ids))
    query = f"select distinct * from players where id in ({placeholders})"

    res = cur.execute(query, player_ids)
    tournaments = res.fetchall()

    columns = table_columns(cur, "players")
    cur.close()

    return to_objects(columns, tournaments)

@app.route("/player/<id>", methods = ["GET"])
def player_by_id(id: str):
    cur = get_db_cursor()

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


@app.route("/tournaments/player/<id>", methods = ["GET"])
def tournaments_by_player(id):
    cur = get_db_cursor()

    columns = table_columns(cur, "tournaments")
    res = cur.execute("select distinct tournament_id from games where white_id = ? or black_id = ?", (id, id))
    tournament_ids = res.fetchall()
    tournament_ids = [t[0] for t in tournament_ids]

    placeholders = ', '.join(['?'] * len(tournament_ids))
    query = f"select * from tournaments where id in ({placeholders})"

    res = cur.execute(query, tournament_ids)
    tournaments = res.fetchall()

    cur.close()

    return to_objects(columns, tournaments)

@app.route("/tournament/<id>", methods = ["GET"])
def tournament_by_id(id: str):
    cur = get_db_cursor()

    columns = table_columns(cur, "tournaments")
    res = cur.execute("select * from tournaments where id = ?", (id,))
    tournament = res.fetchall()

    cur.close()

    return to_objects(columns, tournament)
