from init import init_db

def main():
    if(init_db.create_schema("database.db")):
        init_db.load_data("database.db", "init/games.pgn")

if __name__ == "__main__":
    main()
