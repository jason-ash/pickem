use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    SqlitePool,
};
use std::{path::Path, time::Duration};

use crate::{
    io::{read_games, read_matchups, read_picks, read_players, read_teams},
    models::{Game, Matchup, Pick, Player, Team},
};

pub async fn connect(path: &Path) -> Result<SqlitePool, sqlx::Error> {
    let connect_options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .min_connections(1)
        .idle_timeout(Duration::from_secs(60))
        .connect_with(connect_options)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    Ok(pool)
}

pub async fn seed_data(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let players = read_players()?.filter_map(Result::ok);
    insert_players(pool, players).await?;

    let teams = read_teams()?.filter_map(Result::ok);
    insert_teams(pool, teams).await?;

    let games = read_games()?.filter_map(Result::ok);
    insert_games(pool, games).await?;

    let matchups = read_matchups()?.filter_map(Result::ok);
    insert_matchups(pool, matchups).await?;

    let picks = read_picks()?.filter_map(Result::ok);
    insert_picks(pool, picks).await?;

    Ok(())
}

pub async fn insert_players(
    pool: &SqlitePool,
    players: impl Iterator<Item = Player>,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;

    for player in players {
        sqlx::query("insert or replace into player (name) values (?);")
            .bind(&player.name)
            .execute(&mut *transaction)
            .await?;
    }

    transaction.commit().await?;
    Ok(())
}

pub async fn insert_teams(
    pool: &SqlitePool,
    teams: impl Iterator<Item = Team>,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;

    for team in teams {
        sqlx::query("insert or replace into team (name, abbreviation) values (?, ?);")
            .bind(&team.name)
            .bind(&team.abbreviation)
            .execute(&mut *transaction)
            .await?;
    }

    transaction.commit().await?;
    Ok(())
}

pub async fn insert_games(
    pool: &SqlitePool,
    games: impl Iterator<Item = Game>,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;

    for game in games {
        sqlx::query(
            "
            insert or replace into game (
                week, game_number, game_time, away_team, home_team, away_score, home_score
            ) values (?, ?, ?, ?, ?, ?, ?);",
        )
        .bind(&game.week)
        .bind(&game.game_number)
        .bind(&game.game_time)
        .bind(&game.away_team)
        .bind(&game.home_team)
        .bind(&game.away_score)
        .bind(&game.home_score)
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;
    Ok(())
}

pub async fn insert_matchups(
    pool: &SqlitePool,
    matchups: impl Iterator<Item = Matchup>,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;

    for matchup in matchups {
        sqlx::query(
            "
            insert or replace into matchup (
                week, game_number, player_1, player_2
            ) values (?, ?, ?, ?);",
        )
        .bind(&matchup.week)
        .bind(&matchup.game_number)
        .bind(&matchup.player_1)
        .bind(&matchup.player_2)
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;
    Ok(())
}

pub async fn insert_picks(
    pool: &SqlitePool,
    picks: impl Iterator<Item = Pick>,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;

    for pick in picks {
        sqlx::query(
            "
            insert or replace into pick (
                player_name, week, game_number, winning_team, confidence, method, submission_time_utc, grace_indicator
            ) values (?, ?, ?, ?, ?, ?, ?, ?);",
        )
        .bind(&pick.player_name)
        .bind(&pick.week)
        .bind(&pick.game_number)
        .bind(&pick.winning_team)
        .bind(&pick.confidence)
        .bind(&pick.method)
        .bind(&pick.submission_time_utc)
        .bind(&pick.grace_indicator)
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;
    Ok(())
}
