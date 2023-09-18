"""Simple utilities for pre-processing data."""


def read_raw_matchups() -> list[list[str]]:
    """Return the raw matchups file as a list of list of strings."""
    with open("data/raw_matchups.csv", "r") as content:
        return [line.strip().split(",") for line in content.readlines()]


def parse_players() -> set[str]:
    """Return a list of unique player names from the raw matchup file."""
    return {name for line in read_raw_matchups()[1:] for name in line}


def parse_matchups() -> list[tuple[int, int, str, str]]:
    """Return matchup pairs given an input of a list of (player, *opponents)."""
    matchups = {
        (week + 1, *sorted([player_1, opponent]))
        for [player_1, *opponents] in read_raw_matchups()[1:]
        for week, opponent in enumerate(opponents)
    }

    out = []
    current_week, game_number = 1, 0
    for week, player_1, player_2 in sorted(matchups):
        if week == current_week:
            game_number += 1
        else:
            current_week = week
            game_number = 1
        out.append((week, game_number, player_1, player_2))

    return out


def export_matchups() -> None:
    with open("data/matchups.csv", "w") as outfile:
        outfile.write("week,game_number,player_1,player_2\n")
        for week, game_number, player_1, player_2 in parse_matchups():
            outfile.write(f"{week},{game_number},{player_1},{player_2}\n")


def export_players() -> None:
    with open("data/players.csv", "w") as outfile:
        outfile.write("name\n")
        for name in sorted(parse_players()):
            outfile.write(f"{name}\n")


if __name__ == "__main__":
    export_matchups()
    export_players()
    print(parse_matchups())
