import uuid
from datetime import datetime


def extract_standard_variant_tournaments(lichess_tournaments):
    standard = []
    for tournament in lichess_tournaments:
        if tournament["variant"]["key"] == "standard":
            standard.append(tournament)

    return standard

def lichess_to_tournaments(lichess_tournaments):
    tournaments = []

    for tournament in lichess_tournaments:
        tournaments.append({
            "id": str(uuid.uuid4()),
            "name": tournament["fullName"],
            "location": "Online",
            "date": str(datetime.fromtimestamp(tournament["startsAt"]/1000)),
            "origin": "Lichess"
        })

    return tournaments
