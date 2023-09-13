import asyncio
from chessdotcom.aio import get_player_profile, get_leaderboards, get_player_games_by_month_pgn, Client
import json
from dataclasses import dataclass
import pandas as pd
import re



MOVE_KEY = "Moves"
"""
        Keys that can be find in typical chess.com png file
        
       'Event', 'Site', 'Date', 'Round', 'White',
       'Black', 'Result', 'SetUp', 'FEN', 'CurrentPosition', 'Timezone', 'ECO',
       'ECOUrl', 'UTCDate', 'UTCTime', 'WhiteElo', 'BlackElo', 'TimeControl',
       'Termination', 'StartTime', 'EndDate', 'EndTime', 'Link 
"""
USED_PGN_KEYS = [MOVE_KEY, "Link", "White", "Black", "Event", "Result", "ECO", "WhiteElo", "BlackElo", "Date", "Round"]

    
    
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
                data_dict[key] = value
    return games

#TODO
def load(dataFrames: list[pd.DataFrame]):
    
    #TODO
    return 0
    
    
pgn = extractGameByPlayerNameInTime("GMHikaruOnTwitch", 2021, 4)
games = transformPGN(pgn)
print(games)




    