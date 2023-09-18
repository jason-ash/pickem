create table player (
  name text primary key
) strict;

create table team (
  name text primary key,
  abbreviation text not null
) strict;

create table game (
  week int not null,
  game_number int not null,
  game_time text not null,
  away_team text not null,
  home_team text not null,
  away_score int,
  home_score int,
  primary key (week, game_number),
  foreign key (away_team) references team(name),
  foreign key (home_team) references team(name)
) strict;

create table pick (
  player_name text not null,
  week int not null,
  game_number int not null,
  winning_team text not null,
  confidence int not null,
  method text not null,
  submission_time_utc text default current_timestamp,
  grace_indicator int not null check (grace_indicator in (0, 1)),
  primary key (week, game_number, player_name, submission_time_utc),
  foreign key (player_name) references player(name),
  foreign key (winning_team) references team(name)
) strict;

create table matchup (
  week int not null,
  game_number int not null,
  player_1 text not null,
  player_2 text not null,
  primary key (week, game_number),
  foreign key (player_1) references player(name),
  foreign key (player_2) references player(name)
) strict;

create view matchup_tidy as
  select week, game_number, player_1 as player_name from matchup
  union all
  select week, game_number, player_2 as player_name from matchup
  order by 1, 2, 3
;

create view pick_official as
  with deadline as (
    select week, min(game_time) as earliest_game
    from game
    group by week
  ),
  valid_pick as (
    select p.*
    from pick p join deadline d on p.week = d.week
    where p.submission_time_utc < d.earliest_game or p.grace_indicator = 1
  ),
  ordered_pick as (
    select
      *,
      row_number() over (
        partition by player_name, week, game_number order by submission_time_utc desc
      ) as rn
    from valid_pick
  ),
  latest_pick as (
    select
      player_name,
      week,
      game_number,
      winning_team,
      confidence,
      method,
      submission_time_utc
    from ordered_pick
    where rn = 1
    order by 2, 1
  )
  select * from latest_pick
;

create view pick_outcome as
  with schedule_result as (
    select
      week,
      game_number,
      case
        when away_score is null and home_score is null then null
        when away_score > home_score then away_team
        when away_score < home_score then home_team
        else 'tie'
      end as winner
    from game
  ),
  output as (
    select
      p.week,
      p.game_number,
      p.player_name,
      p.winning_team,
      p.confidence,
      case
        when p.winning_team = s.winner then 1
        when s.winner = 'tie' then 1
        else 0
      end as correct,
      case
        when p.winning_team = s.winner then p.confidence
        when s.winner = 'tie' then p.confidence
        else 0
      end as points
    from schedule_result s join pick_official p on s.week = p.week and s.game_number = p.game_number
    order by 1, 3, 2
  )
  select * from output
;

create view pick_score as
  with points as (
    select week, player_name, sum(points) as points
    from pick_outcome
    group by 1, 2
  ),
  output as (
    select m.*, p.points
    from matchup_tidy m
      left join points p
      on m.week = p.week and m.player_name = p.player_name
    order by 1, 2
  )
  select * from output
;

create view pick_statistic as
  with ordered_score as (
    select
      week,
      points,
      row_number() over (partition by week order by points) as rn,
      count(*) over (partition by week) as c
    from pick_score
  ),
  score_median as (
    select
      distinct(week),
      avg(
        case c % 2
          when 0 then case when rn in (c / 2, c / 2 + 1) then points end
          when 1 then case when rn = c / 2 + 1 then points end
        end
      ) over (partition by week) as score_med
    from ordered_score
  ),
  score_distribution as (
    select
      week,
      min(points) as score_min,
      avg(points) as score_avg,
      max(points) as score_max
    from pick_score
    group by 1
  ),
  output as (
    select
      d.week,
      d.score_min,
      d.score_avg,
      m.score_med,
      d.score_max
    from score_distribution d join score_median m on d.week = m.week
  )
  select * from output
;

create view matchup_outcome as
  with output as (
    select
      m.*,
      p1.points as player_1_score,
      p2.points as player_2_score,
      case
        when p1.points is null and p2.points is null then null
        when p1.points is null and p2.points is not null then 0
        when p1.points is not null and p2.points is null then 10
        when p1.points > p2.points then 10
        else 0
      end as player_1_points,
      case
        when p1.points is null and p2.points is null then null
        when p1.points is null and p2.points is not null then 10
        when p1.points is not null and p2.points is null then 0
        when p1.points < p2.points then 10
        else 0
      end as player_2_points
    from matchup m
      left join pick_score p1 on m.week = p1.week and m.player_1 = p1.player_name
      left join pick_score p2 on m.week = p2.week and m.player_2 = p2.player_name
  )
  select * from output
;

create view matchup_score as
  with points as (
    select week, game_number, player_1 as player_name, player_1_points as points
    from matchup_outcome
    union all
    select week, game_number, player_2 as player_name, player_2_points as points
    from matchup_outcome
  )
  select * from points
  order by 1, 2, 3
;

create view mercy_score as
  select
    p.week,
    p.game_number,
    p.player_name,
    case
      when p.points is null then s.score_min - 1
      else 0
    end as points
    from pick_score p join pick_statistic s on p.week = s.week
    order by 1, 2
;

create view score as
  select
    p.week,
    p.player_name,
    coalesce(p.points, 0) as pick_points,
    coalesce(m.points, 0) as matchup_points,
    coalesce(c.points, 0) as mercy_points,
    coalesce(p.points, 0) + coalesce(m.points, 0) + coalesce(c.points, 0) as total_points
  from pick_score p
    join matchup_score m on p.week = m.week and p.player_name = m.player_name
    join mercy_score c on p.week = c.week and p.player_name = c.player_name
  order by 1, 2
;

create view leaderboard as
  with s as (
    select
      player_name,
      sum(pick_points) as pick_points,
      sum(matchup_points) as matchup_points,
      sum(mercy_points) as mercy_points,
      sum(total_points) as total_points
    from score
    where week < 10
    group by 1
  ),
  output as (
    select
      rank() over (order by total_points desc) as position,
      player_name,
      pick_points,
      matchup_points,
      mercy_points,
      total_points
    from s
  )
  select * from output
;

create view pick_analysis as
  with pick_metric as (
    select
      week,
      winning_team,
      count(*) as votes,
      sum(confidence) as confidence_sum
    from pick_official
    group by 1, 2
  ),
  aggregated as (
    select
      g.week,
      g.game_number,
      g.away_team,
      g.home_team,
      coalesce(p1.votes, 0) as away_votes,
      coalesce(p2.votes, 0) as home_votes,
      coalesce(p1.confidence_sum, 0) as away_confidence_sum,
      coalesce(p2.confidence_sum, 0) as home_confidence_sum
    from game g
      left join pick_metric p1 on g.week = p1.week and g.away_team = p1.winning_team
      left join pick_metric p2 on g.week = p2.week and g.home_team = p2.winning_team
  ),
  output as (
    select
      week,
      game_number,
      away_team,
      home_team,
      away_votes,
      home_votes,
      away_confidence_sum,
      home_confidence_sum,
      round(1.0 * away_votes / (away_votes + home_votes), 5) as away_win_percent,
      round(1.0 * home_votes / (away_votes + home_votes), 5) as home_win_percent,
      coalesce(round(1.0 * away_confidence_sum / away_votes, 5), 0.0) as away_confidence_avg,
      coalesce(round(1.0 * home_confidence_sum / home_votes, 5), 0.0) as home_confidence_avg
    from aggregated
  )
  select * from output
;

create view pick_consensus as
  select
    week,
    game_number,
    case
      when away_confidence_sum > home_confidence_sum then away_team
      else home_team
    end as winning_team,
    case
      when away_confidence_sum < home_confidence_sum then away_confidence_avg
      else home_confidence_avg
    end as confidence_avg
  from pick_analysis
;
