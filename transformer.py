import uuid
from datetime import date


def extract_standard_variant_tournaments(lichess_tournaments):
    standard = []

    for tournament in lichess_tournaments:
        if tournament["variant"]["key"] == "standard":
            standard.append(tournament)

    return standard

def lichess_to_tournaments(lichess_tournaments):
    tournaments = []

    for tournament in lichess_tournaments:
        tournament_date = str(date.fromtimestamp(tournament["startsAt"]/1000))
        tournament_date = tournament_date.replace('-', '.')
        tournaments.append({
            "id": str(uuid.uuid4()),
            "name": tournament["fullName"],
            "location": "lichess.org INT",
            "date": tournament_date,
            "origin": "Lichess",
            "lichess_id": tournament["id"]
        })

    return tournaments

def transform_result(result):
    result = result.strip()
    if result == "1-0":
        return "White"
    if result == "0-1":
        return "Black"
    if result == "1/2-1/2":
        return "Draw"
    else:
        return "Unfinished/Unknown"
