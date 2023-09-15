create table player (
  name text primary key
) strict;

create table team (
  name text primary key,
  abbreviation text not null
) strict;

create table schedule (
  week int not null,
  game int not null,
  game_time text not null,
  away_team text not null,
  home_team text not null,
  away_score int,
  home_score int,
  primary key (week, game),
  foreign key (away_team) references team(name),
  foreign key (home_team) references team(name)
) strict;

create table pick (
  player_name text not null,
  week int not null,
  game int not null,
  winning_team text not null,
  confidence int not null,
  method text not null,
  submission_time_utc text default current_timestamp,
  grace_ind int not null check (grace_ind in (0, 1)),
  primary key (week, game, player_name, submission_time_utc),
  foreign key (player_name) references player(name),
  foreign key (winning_team) references team(name)
) strict;

create table matchup (
  week int not null,
  game int not null,
  player_1 text not null,
  player_2 text not null,
  primary key (week, game),
  foreign key (player_1) references player(name),
  foreign key (player_2) references player(name)
) strict;
