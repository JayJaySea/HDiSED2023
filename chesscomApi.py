import asyncio
from chessdotcom.aio import get_player_profile, get_leaderboards, get_player_profile, get_player_games_by_month_pgn, get_player_game_archives, Client
import json
from dataclasses import dataclass
import pandas as pd
import re
from misc import *
import uuid



MOVE_KEY = "Moves"
"""
        Keys that can be find in typical chess.com png file
        
       'Event', 'Site', 'Date', 'Round', 'White',
       'Black', 'Result', 'SetUp', 'FEN', 'CurrentPosition', 'Timezone', 'ECO',
       'ECOUrl', 'UTCDate', 'UTCTime', 'WhiteElo', 'BlackElo', 'TimeControl',
       'Termination', 'StartTime', 'EndDate', 'EndTime', 'Link 
"""
USED_PGN_KEYS = [MOVE_KEY, "TimeControl", "Link", "White", "Black", "Event", "Result", "ECO", "ECOUrl","WhiteElo", "BlackElo", "Date", "Round"]

    

@app.route("/games/player/chesscom/<name>/<year>/<month>", methods = ["GET"])
def get_chesscom_player_games(name, year, month):
    pgn = extractGameByPlayerNameInTime(name, year, month)
    games = transformPGN(pgn)
    load(games)

    games_json = []
    for game in games:
        games_json.append(game.to_dict(orient='records'))

    return games_json
    
def extractGameByPlayerNameInTime(playerName: str, year: int, month: int = 1):
    """Extract from chess.api player games in specified time

    Args:
        playerName (str): name of player
        year (int): year
        month (int, optional): month. Defaults to 1(January).

    Returns:
        str: pgn of games 
    """
    responsesStr = ""
    Client.aio = True
    try:
        cors = [get_player_games_by_month_pgn(playerName, year, month)]
        async def gather_cors(cors):
            responses = await asyncio.gather(*cors)
            return responses

        responses = asyncio.run(gather_cors(cors))
        responsesStr = json.dumps(responses[0].json, indent=2)
    except Exception as error:
        print(error)

    return responsesStr
    
def extractPlayerProfile(playerName: str):
    responsesStr = ""
    Client.aio = True

    try:
        cors = [get_player_profile(playerName)]
        async def gather_cors(cors):
            responses = await asyncio.gather(*cors)
            return responses

        responses = asyncio.run(gather_cors(cors))
        responsesStr = json.dumps(responses[0].json, indent=2)
    except Exception as error:
        print(error)
    return responsesStr

def transformPlayerProfile(data):
    df = None
    data_dict = {}
   

    keys = ["name", "id", "title"]
    values = []
    if 'username' in data["player"]:
        values.append(data["player"]["username"])
       
    elif 'username' in data["player"]:
        values.append(data["player"]["username"])
    else:
        raise ValueError("cannot find player")
    id = uuid.uuid4()
    values.append(id)
    
    # if player has a country uncomment
    #if 'country' in data["player"]:
    #    country = data["player"]["country"]
    #    country = country.split('/')[-1]
    #    values.append(country)
    #else:
    #    values.append(None)
        
    if 'title' in data["player"]:
        values.append(data["player"]["title"])
    else:
        values.append(None)    
        
    
   
    try:
        for i in range(len(keys)):
            data_dict[keys[i]] = values[i]
        df = pd.DataFrame([data_dict])
    except Exception as error:
        print(error)
    return df

def loadPlayer(player: pd.DataFrame):
    values = []
    cur = get_db_cursor()
    id = str(uuid.uuid4())
 
    try: 
        
        values.append((
            #str(player["id"].values[0]),
            id,
            str(player["name"].values[0]),
            None,
            player["title"].values[0]
        ))
        
        
    except Exception as error:
        print(error)
   
    cur.executemany("insert into players values(?, ?, ?, ?)", values)
    cur.connection.commit()
    cur.close()
    return id
    
    


def transformMoves(moves: str):
    """Function adjust chess.com moves notation to lichess notation
    Args:
        moves (str): moves consistent with the chess.com notation
    Returns:
        str: transformed moves
    """
    moveTimestampPattern = r'\{[^}]*\}'
    threeDotsPattern = r'[.][.][.]'
    moves = re.sub(moveTimestampPattern, '', moves)
    moves = re.sub(threeDotsPattern, '.', moves)
    return moves   



def transformPGN(pgn: str):
    """Function transforms data from pgn into Game object
    Args:
        pgn (str): raw pgn file 

    Returns:
        <class 'list'>: list of DataFrame for each game
    """
    lines = pgn.split("\\n")
    games = []
    data_dict = {}
    for line in lines:
        if len(line) == 0:
            continue
        #finding line with moves
        if(line[0] == "1"):
            if MOVE_KEY in USED_PGN_KEYS:
                moves = transformMoves(line)
                data_dict[MOVE_KEY] = moves
            df = pd.DataFrame([data_dict])
            games.append(df)
            data_dict = {}
            continue
            
        parts = line.strip('[]').split(' \\"')
        #transform key value field
        if len(parts) == 2:
            parts[1] = parts[1].strip('\\\\"')
            key, value = parts
            if key in USED_PGN_KEYS:
                if key == 'Round' and value != '-':
                    data_dict['Event'] = None
                    continue
                if key == 'Result':
                    data_dict[key] = transform_result(value)
                data_dict[key] = value
    return games

def transform_result(result: str):
    result = result.strip()
    if result == "1-0":
        return "White"
    if result == "0-1":
        return "Black"
    if result == "1/2-1/2":
        return "Draw"
    else:
        return "Unfinished/Unknown"

def checkIfPlayerExists(playerName: str):
    playerName = playerName.lower()
    cur = get_db_cursor()
    query = "select count(*) from players where lower(name) = ?"
    cur.execute(query, (playerName,))
    result = cur.fetchone()
  
    if result[0] > 0: 
        return True
    return False

def checkIfGameExists(gameOrigin: str):
    gameOrigin = gameOrigin.lower()
    cur = get_db_cursor()
    query = "select count(*) from games where lower(origin) = ?"
    cur.execute(query, (gameOrigin,))
    result = cur.fetchone()
  
    if result[0] > 0: 
        return True
    return False
    
    

def getPlayerIdOrNone(playerName: str):
    playerName = playerName.lower()
    cur = get_db_cursor()
    query = "select id from players where lower(name) = ?"
    cur.execute(query, (playerName,))
    result = cur.fetchone()
    if result != None: 
        return result[0]
    return None




def load(games: list[pd.DataFrame]):
    
 
    values = []
    
   
    for game in games:
   
        
        whiteName = str(game["White"].values[0])
        blackName = str(game["Black"].values[0])
      
        if checkIfPlayerExists(whiteName) == True:
            
            whiteId = getPlayerIdOrNone(whiteName)
           
            
        else:
           
           
            playerProfile = json.loads(extractPlayerProfile(whiteName))
          
            
            playerDf = transformPlayerProfile(playerProfile)
            whiteId = loadPlayer(playerDf)
            #whiteId = playerDf["id"]
           
            
        blackId = ""
        if checkIfPlayerExists(blackName) == True:
    
            blackId = getPlayerIdOrNone(blackName)
    
            
            
        else:
            playerProfile = json.loads(extractPlayerProfile(blackName))
            playerDf = transformPlayerProfile(playerProfile)
            blackId = loadPlayer(playerDf)
            #blackId = playerDf["id"]
        
        try: 
            
            if not checkIfGameExists(str(game["Link"].values[0])):
                values.append((
                    str(uuid.uuid4()),
                    str(whiteId),
                    str(blackId),
                    None,
                    str(game["Result"].values[0]),
                    str(game["Moves"].values[0]),
                    int(game["WhiteElo"].values[0]),
                    int(game["BlackElo"].values[0]),
                    str(game["TimeControl"].values[0]),
                    str(game["Date"].values[0]),
                    str(game["ECOUrl"].values[0]),
                    str(game["ECO"].values[0]),
                    str(game["Link"].values[0]),
                ))
        except Exception as error:
            print(error)
   
    cur = get_db_cursor()
    cur.executemany("insert into games values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
    cur.connection.commit()
    cur.close()
    
