from misc import *
import uuid
from time import sleep
import multiprocessing
from transformer import *
import requests
import json

lock = multiprocessing.Lock()

@app.route("/tournaments/recent", methods = ["GET"])
def recent_tournaments():
    try:
        data = requests.get("https://lichess.org/api/tournament", timeout=5).text
        recent = json.loads(data)["finished"]
    except:
        return []

    standard = extract_standard_variant_tournaments(recent)
    tournaments = lichess_to_tournaments(standard)

    cur = get_db_cursor()
    unique = leave_unique_tournaments(cur, tournaments)
    insert_tournaments(cur, unique)

    completing_data = multiprocessing.Process(
        target=start_completing_data, args=(cur, [unique[0]], lock)
    )
    completing_data.start()

    cur.close()

    return tournaments

def leave_unique_tournaments(cur, tournaments):
    filtered = []
    for tournament in tournaments:
        count = cur.execute(
            "select count(*) from tournaments where date = ? and name = ?",
            (tournament["date"], tournament["name"])
        ).fetchall()[0][0]
        
        if count == 0:
            filtered.append(tournament)

    return filtered

def start_completing_data(cur, tournaments, lock):
    if lock.acquire(block=False):
        try:
            games = fetch_new_games(tournaments)
            games = parse_games(games)

            extracted = extract_players(games)
            all_players, new_players = extract_new_players(cur, extracted)
            new_players = extracted_to_players(new_players)
            insert_players(cur, new_players)

            games = extracted_to_games(games, all_players)
            insert_games(cur, games)

            print(games)
        finally:
            lock.release()
    else:
        return

def fetch_new_games(tournaments):
    print("Fetching: " + str(tournaments))
    
    games = {}
    for tournament in tournaments:
        data = ""
        while True:
            try:
                res = requests.get(f"https://lichess.org/api/tournament/{tournament['lichess_id']}/games", timeout=5)
            except:
                continue

            if res.ok:
                data = res.text
                break
            else:
                print("Got status " + str(res.status_code) + ". Waiting..")
                sleep(61.)

        games[tournament["id"]] = data
        print("Fetched: " + games[tournament["id"]])

    return games

def parse_games(games):
    print("started parsing")
    parsed = []
    game = {}

    for tournament_id, tournament_games in games.items():
        for line in tournament_games.splitlines():
            if not line.strip():
                continue

            if line[0] == "[":
                parts = line.strip()[1:-1].split(' "')
                if len(parts) == 2:
                    key, value = parts
                    if key.strip() == "WhiteElo" or key.strip() == "BlackElo":
                        try:
                            game[key] = int(value[:-1])
                        except ValueError:
                            game[key] = None
                    else:
                        game[key] = value[:-1]

            elif line[0] == '1' or line[0] == '0':
                game["TournamentID"] = tournament_id
                game["Moves"] = line.strip()
                game["Winner"] = transform_result(game.get("Result"))
                game["Id"] = str(uuid.uuid4())
                parsed.append(game)
                game = {}

    return parsed

def extract_players(games):
    # Using dict to ensure unique players
    players = {}
    print("extracting players")

    for game in games:
        try:
            players[game["White"]] = str(uuid.uuid4())
            players[game["Black"]] = str(uuid.uuid4())
        except:
            pass

    return players

def extract_new_players(cur, extracted):
    new_players = {}
    all_players = {}

    for name, uuid in extracted.items():
        existing_id = cur.execute(
            "select id from players where name = ?",
            (name,)
        )
        existing_id = existing_id.fetchone()

        if existing_id:
            all_players[name] = existing_id[0]
        else:
            new_players[name] = uuid
            all_players[name] = uuid

    return all_players, new_players

def extracted_to_players(extracted):
    print("creating players")
    players = []

    for name, uuid in extracted.items():
        data = "[]"
        while True:
            try:
                sleep(1.)
                print(name.strip())
                res = requests.get("https://lichess.org/api/user/" + name.strip())
                print(res.text)

                if res.ok: 
                    data = json.loads(res.text)
                    print("Player received: " + str(data))
                    break
                else:
                    print("Got status " + str(res.status_code) + ". Waiting..")
                    # print("Data received: " + res.text)
                    sleep(61.)
            except Exception as error:
                print(error)
                continue

        players.append({
            "id": uuid,
            "name": name,
            "ranking": data.get("profile", {}).get("fideRating"),
            "title": data.get("title")
        })

        print("Created: " + str(players[-1]))

    return players

def insert_players(cur, players):
    cur.executemany(
        "insert into players values(:id, :name, :ranking, :title)",
        players
    )

    cur.connection.commit()

def extracted_to_games(extracted_games, players):
    games = []
    for game in extracted_games:
        games.append({
            "id": game["Id"],
            "white_id": players[game["White"]],
            "black_id": players[game["Black"]],
            "tournament_id": game["TournamentID"],
            "winner": game["Winner"],
            "moves": game["Moves"],
            "white_elo": game["WhiteElo"],
            "black_elo": game["BlackElo"],
            "time_format": game["TimeControl"],
            "date": game["Date"],
            "debut": None,
            "eco": game["ECO"],
            "origin": game.get("Site")
        })

    return games

def insert_tournaments(cur, tournaments):
    cur.executemany(
        "insert into tournaments values(:id, :name, :location, :date, :origin)",
        tournaments
    )

    cur.connection.commit()

def insert_games(cur, games):
    print(games)
    cur.executemany(
        "insert into games values(:id, :white_id, :black_id, :tournament_id, :winner, :moves, :white_elo, :black_elo, :time_format, :date, :debut, :eco, :origin)",
        games
    )

    cur.connection.commit()
