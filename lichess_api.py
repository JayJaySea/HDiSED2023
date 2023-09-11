from misc import *
from transformer import extract_standard_variant_tournaments, lichess_to_tournaments
import requests
import json

@app.route("/tournaments/ongoing", methods = ["GET"])
def ongoing_tournaments():
    data = requests.get("https://lichess.org/api/tournament", timeout=5)
    data = data.text
    tournaments_raw = json.loads(data)
    ongoing = tournaments_raw["started"]
    standard = extract_standard_variant_tournaments(ongoing)
    tournaments = lichess_to_tournaments(standard)

    cur = get_db_cursor()
    cur.close()

    return tournaments
