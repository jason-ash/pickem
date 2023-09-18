use crate::models::{Game, Matchup, Pick, Player, Team};
use std::path::Path;

pub fn read_players() -> Result<impl Iterator<Item = Result<Player, csv::Error>>, std::io::Error> {
    let reader = csv::Reader::from_path(Path::new("./data/players.csv"))?;
    Ok(reader.into_deserialize())
}

pub fn read_teams() -> Result<impl Iterator<Item = Result<Team, csv::Error>>, std::io::Error> {
    let reader = csv::Reader::from_path(Path::new("./data/teams.csv"))?;
    Ok(reader.into_deserialize())
}

pub fn read_games() -> Result<impl Iterator<Item = Result<Game, csv::Error>>, std::io::Error> {
    let reader = csv::Reader::from_path(Path::new("./data/games.csv"))?;
    Ok(reader.into_deserialize())
}

pub fn read_matchups() -> Result<impl Iterator<Item = Result<Matchup, csv::Error>>, std::io::Error>
{
    let reader = csv::Reader::from_path(Path::new("./data/matchups.csv"))?;
    Ok(reader.into_deserialize())
}

pub fn read_picks() -> Result<impl Iterator<Item = Result<Pick, csv::Error>>, std::io::Error> {
    let reader = csv::Reader::from_path(Path::new("./data/picks.csv"))?;
    Ok(reader.into_deserialize())
}
