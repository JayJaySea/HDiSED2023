import sqlite3
import uuid
import chess.pgn
import os, mmap
from io import StringIO

def create_schema(database_path):
    if os.path.exists(database_path):
        msg = '''Will remove existing database and repopulate it.\n
Do you want to proceed? [Y/n] '''
        print(msg, end='')

        decision = input().lower()
        if decision == "n":
            return False

        msg = "Removing existing database"
        print(msg)

        os.remove("./database.db")

    con = sqlite3.connect(database_path)
    cur = con.cursor()

    msg = "Creating players table... "
    print(msg, end='')
    cur.execute('''
    CREATE TABLE players(
        id text not null primary key,
        name text not null,
        ranking integer,
        title text
    )
    ''')
    print("Done")

    msg = "Creating tournaments table... "
    print(msg, end='')
    cur.execute('''
    CREATE TABLE tournaments(
        id text not null primary key,
        name text not null,
        location text,
        date text,
        origin text
    )
    ''')
    print("Done")

    msg = "Creating games table... "
    print(msg, end='')
    cur.execute('''
    CREATE TABLE games(
        id text not null primary key,
        white_id text not null references players(id) on delete restrict,
        black_id text not null references players(id) on delete restrict,
        tournament_id text references tournaments(id) on delete restrict,
        result text not null,
        moves text not null,
        time_format text,
        date text,
        debut text,
        eco text
    )
    ''')
    print("Done")

    return True

def load_data(database_path, file):
    pgn_filename = file
    con = sqlite3.connect(database_path)
    cur = con.cursor()

    games = []
    with open(pgn_filename, "r+") as pgn_file:
        games = load_games(pgn_file)
        print("Loading games from file... Done")

    players,tournaments = extract_players_and_tournaments(games)
    insert_players(cur, players)
    insert_tournaments(cur, tournaments)
    con.commit()
    print("Populating players and tournaments... Done")

    games = extract_games(games, players, tournaments)
    insert_games(cur, games)
    con.commit()
    print("Populating games... Done")


def load_games(pgn_file):
    games = []
    i = 0
    count = 910877
    
    games_mmap = mmap.mmap(pgn_file.fileno(), 0)

    game = {}
    for line in iter(games_mmap.readline, b""):
        line = line.decode("utf-8")

        if not line.strip():
            continue

        if line[0] == "[":
            parts = line.strip()[1:-1].split(' "')
            if len(parts) == 2:
                key, value = parts
                game[key] = value[:-1]
        elif line[0] == '1':
            game["Moves"] = line
            games.append(game)
            game = {}

            progress = str(int((i/count)*100)) + "%"
            print("Loading games from file... " + progress, end="\r")
            i += 1

    return games

def extract_players_and_tournaments(games):
    players = {}
    tournaments = {}
    i = 0
    count = len(games)

    for game in games:
        try:
            players[game["White"]] = uuid.uuid4()
            players[game["Black"]] = uuid.uuid4()
        except:
            pass

        try:
            tournaments[game["Event"]] = {
                "id": uuid.uuid4(),
                "site": game.get("Site"),
                "date": game.get("Date"),
            }
        except:
            pass

        progress = str(int((i/count)*100)) + "%"
        print("Populating players and tournaments... " + progress, end="\r")
        i += 1

    return players, tournaments


def insert_players(cur, players):
    values = []
    for name, uuid in players.items():
        values.append((str(uuid), name, None, 'GM'))

    cur.executemany("insert into players values(?, ?, ?, ?)", values)

def insert_tournaments(cur, tournaments):
    values = []
    for name, data in tournaments.items():
        values.append((str(data["id"]), name, data["site"], data["date"], 'Historical'))

    cur.executemany("insert into tournaments values(?, ?, ?, ?, ?)", values)

def extract_games(games, players, tournaments):
    extracted = []
    i = 0
    count = len(games)
    for game in games:
        try:
            extracted_game = {
                "id": uuid.uuid4(),
                "white": players[game["White"]],
                "black": players[game["Black"]],
                "tournament": tournaments[game["Event"]]["id"],
                "result": game.get("Result"),
                "moves": game.get("Moves"),
                "date": game.get("Date"),
                "eco": game.get("ECO")
            }
            extracted.append(extracted_game)
        except:
            pass

        progress = str(int((i/count)*100)) + "%"
        print("Populating games... " + progress, end="\r")
        i += 1

    return extracted

def insert_games(cur, games):
    values = []
    for game in games:
        values.append(
            (str(game["id"]),
             str(game["white"]),
             str(game["black"]),
             str(game["tournament"]),
             game["result"],
             game["moves"],
             'Classical',
             game["date"],
             None,
             game["eco"]
             )
        )

    cur.executemany("insert into games values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
