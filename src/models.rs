use serde::{
    de::{self, Unexpected},
    Deserialize, Deserializer,
};

#[derive(Debug, Deserialize)]
pub struct Player {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct Team {
    pub name: String,
    pub abbreviation: String,
}

#[derive(Debug, Deserialize)]
pub struct Game {
    pub week: u8,
    pub game_number: u8,
    pub game_time: String,
    pub away_team: String,
    pub home_team: String,
    pub away_score: Option<u8>,
    pub home_score: Option<u8>,
}

#[derive(Debug, Deserialize)]
pub struct Matchup {
    pub week: u8,
    pub game_number: u8,
    pub player_1: String,
    pub player_2: String,
}

#[derive(Debug, Deserialize)]
pub struct Pick {
    pub player_name: String,
    pub week: u8,
    pub game_number: u8,
    pub winning_team: String,
    pub confidence: u8,
    pub method: String,
    pub submission_time_utc: String,
    #[serde(deserialize_with = "int_to_bool")]
    pub grace_indicator: bool,
}

fn int_to_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer)?.as_str() {
        "0" => Ok(true),
        "1" => Ok(false),
        other => Err(de::Error::invalid_value(
            Unexpected::Str(other),
            &"Must be either 0 or 1.",
        )),
    }
}
