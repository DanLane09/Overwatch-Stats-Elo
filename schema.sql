-- ===============================
-- OWCS ELO + Player Tracking Schema
-- ===============================

-- Drop all existing tables (optional — useful for development resets)
DROP TABLE IF EXISTS elo_history CASCADE;
DROP TABLE IF EXISTS roster_changes CASCADE;
DROP TABLE IF EXISTS player_team_history CASCADE;
DROP TABLE IF EXISTS player_map_stats CASCADE;
DROP TABLE IF EXISTS maps_played CASCADE;
DROP TABLE IF EXISTS player_match_stats CASCADE;
DROP TABLE IF EXISTS matches CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS teams CASCADE;

--------- TEAMS
CREATE TABLE teams (
    team_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    region VARCHAR(20) NOT NULL,                   -- e.g. 'NA', 'EMEA', 'KR'
    current_elo FLOAT DEFAULT 1000,
    active BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

--------- PLAYERS
CREATE TABLE players (
    player_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    nationality VARCHAR(2),
    role VARCHAR(10),
    current_team_id INT REFERENCES teams(team_id) ON DELETE SET NULL,
    current_elo FLOAT DEFAULT 1000,
    active BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

--------- TOURNAMENTS
CREATE TABLE tournaments (
    tournament_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(50),
    region VARCHAR(50),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    format TEXT,
    prize_pool_USD FLOAT
);

CREATE TABLE tournament_teams (
    tournament_id INT REFERENCES tournaments(tournament_id),
    team_id INT REFERENCES teams(team_id),
    seed INT,
    final_placement INT
);

--------- MATCHES
CREATE TABLE matches (
    match_id SERIAL PRIMARY KEY,
    date_played TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    tournament_id INT REFERENCES tournaments(tournament_id),
    match_type VARCHAR(50) NOT NULL,
    game_version VARCHAR(50),
    first_to INT DEFAULT 3,
    upper_seed_team_id INT REFERENCES teams(team_id),
    lower_seed_team_id INT REFERENCES teams(team_id),
    upper_seed_team_score INT DEFAULT 0,
    lower_seed_team_score INT DEFAULT 0,
    winning_team_id INT REFERENCES teams(team_id),
    losing_team_id INT REFERENCES teams(team_id)
);

--------- HEROES
CREATE TABLE heroes (
    hero_id SERIAL PRIMARY KEY,
    hero_name VARCHAR(50) NOT NULL
);
INSERT INTO heroes (hero_name)
VALUES
('Ana'), ('Anran'), ('Ashe'), ('Baptiste'), ('Bastion'), ('Brigitte'), ('Cassidy'), ('D.Va'), ('Domina'), ('Doomfist'), ('Echo'), ('Emre'), ('Freja'),
('Genji'), ('Hanzo'), ('Hazard'), ('Illari'), ('Jetpack Cat'), ('Junker Queen'), ('Junkrat'), ('Juno'), ('Kiriko'), ('Lifeweaver'),
('Lucio'), ('Mauga'), ('Mei'), ('Mercy'), ('Mizuki'), ('Moira'), ('Orisa'), ('Pharah'), ('Ramattra'), ('Reaper'), ('Reinhardt'),
('Roadhog'), ('Shion'), ('Sierra'), ('Sigma'), ('Sojourn'), ('Soldier: 76'), ('Sombra'), ('Symmetra'), ('Torbjorn'), ('Tracer'), ('Vendetta'),
('Venture'), ('Widowmaker'), ('Winston'), ('Wrecking Ball'), ('Wuyang'), ('Zarya'), ('Zenyatta');

--------- MAP TYPES
CREATE TABLE map_types (
    map_type_id SERIAL PRIMARY KEY,
    map_type VARCHAR(15)
);
INSERT INTO map_types (map_type)
VALUES
('Clash'), ('Control'), ('Escort'), ('Flashpoint'), ('Hybrid'), ('Push');

--------- MAPS
CREATE TABLE maps (
    map_id SERIAL PRIMARY KEY,
    map_name VARCHAR(50),
    map_type_id INT REFERENCES map_types(map_type_id)
);
INSERT INTO maps (map_name, map_type_id)
VALUES
('Hanaoka', 1), ('Throne of Anubis', 1),
('Antarctic Peninsula', 2), ('Busan', 2), ('Ilios', 2), ('Lijiang Tower', 2), ('Nepal', 2), ('Oasis', 2), ('Samoa', 2),
('Circuit Royal', 3), ('Dorado', 3), ('Havana', 3), ('Junkertown', 3), ('Rialto', 3), ('Route 66', 3), ('Shambali Monastery', 3), ('Watchpoint: Gibraltar', 3),
('Aatlis', 4), ('New Junk City', 4), ('Suravasa', 4),
('Blizzard World', 5), ('Eichenwalde', 5), ('Hollywood', 5), ('King''s Row', 5), ('Midtown', 5), ('Neon Junction', 5), ('Numbani', 5), ('Paraiso', 5),
('Colosseo', 6), ('Esperanca', 6), ('New Queen Street', 6), ('Runasapi', 6);

--------- MAPS PLAYED (each map in a match)
CREATE TABLE maps_played (
    map_played_id SERIAL PRIMARY KEY,
    match_id INT REFERENCES matches(match_id) ON DELETE CASCADE,
    map_number INT NOT NULL,                       -- 1, 2, 3...
    map_pick_team_id INT REFERENCES teams(team_id),
    map_id INT REFERENCES maps(map_id),
    replay_code VARCHAR(10),
    blue_team_id INT REFERENCES teams(team_id),
    red_team_id INT REFERENCES teams(team_id),
    blue_team_score FLOAT DEFAULT 0,
    red_team_score FLOAT DEFAULT 0,
    winning_team_id INT REFERENCES teams(team_id),
    losing_team_id INT REFERENCES teams(team_id),
    deferred_first_ban BOOLEAN DEFAULT FALSE,
    first_ban_team_id INT REFERENCES teams(team_id),
    first_ban_hero_id INT REFERENCES heroes(hero_id),
    second_ban_hero_id INT REFERENCES heroes(hero_id),
    map_length_seconds INT,
    UNIQUE (match_id, map_number)
);

--------- PLAYER MAP STATS
CREATE TABLE player_map_stats (
    id SERIAL PRIMARY KEY,
    map_played_id INT REFERENCES maps_played(map_played_id) ON DELETE CASCADE,
    player_id INT REFERENCES players(player_id) ON DELETE CASCADE,
    team_id INT REFERENCES teams(team_id),
    opponent_id INT REFERENCES teams(team_id),
    fantasy_score FLOAT DEFAULT 0,
    eliminations INT DEFAULT 0,
    deaths INT DEFAULT 0,
    assists INT DEFAULT 0,
    damage INT DEFAULT 0,
    healing INT DEFAULT 0,
    mitigated INT DEFAULT 0,
    UNIQUE (map_played_id, player_id)
);

CREATE TABLE player_hero_map_stats (
    id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id) ON DELETE CASCADE,
    map_played_id INT REFERENCES maps_played(map_played_id) ON DELETE CASCADE,
    team_id INT REFERENCES teams(team_id),
    opponent_id INT REFERENCES teams(team_id),
    hero_id INT REFERENCES heroes(hero_id) ON DELETE CASCADE,
    seconds_played INT DEFAULT 0,
    eliminations INT DEFAULT 0,
    assists INT DEFAULT 0,
    deaths INT DEFAULT 0,
    damage INT DEFAULT 0,
    healing INT DEFAULT 0,
    mitigated INT DEFAULT 0
);


-- TIME SERIES DATA
CREATE TABLE player_timeseries (
    id BIGSERIAL PRIMARY KEY,
    map_played_id INT REFERENCES maps_played(map_played_id),
    player_id INT REFERENCES players(player_id),
    team_id INT REFERENCES teams(team_id),
    time_seconds FLOAT,
    hero_id INT REFERENCES heroes(hero_id),
    minor_perk TEXT DEFAULT NULL,
    major_perk TEXT DEFAULT NULL,
    ult_charged BOOLEAN,
    eliminations INT,
    deaths INT,
    assists INT,
    damage INT,
    healing INT,
    mitigated INT
);

--------- PLAYER TEAM HISTORY
CREATE TABLE player_team_history (
    history_id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id) ON DELETE CASCADE,
    team_id INT REFERENCES teams(team_id) ON DELETE CASCADE,
    start_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    end_date TIMESTAMP WITH TIME ZONE,
    UNIQUE (player_id, team_id, start_date)
);

--------- ROSTER CHANGES
CREATE TABLE roster_changes (
    change_id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id) ON DELETE CASCADE,
    old_team_id INT REFERENCES teams(team_id) ON DELETE SET NULL,
    new_team_id INT REFERENCES teams(team_id) ON DELETE SET NULL,
    change_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    reason TEXT
);

--------- ELO HISTORY
CREATE TABLE player_elo_history (
    elo_id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id),
    old_elo FLOAT NOT NULL,
    new_elo FLOAT NOT NULL,
    delta FLOAT GENERATED ALWAYS AS (new_elo - old_elo) STORED,
    reason TEXT,
    date_recorded TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE team_elo_history (
    elo_id SERIAL PRIMARY KEY,
    team_id INT NOT NULL,
    old_elo FLOAT NOT NULL,
    new_elo FLOAT NOT NULL,
    delta FLOAT GENERATED ALWAYS AS (new_elo - old_elo) STORED,
    reason TEXT,
    date_recorded TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ===============================
-- EVENT TABLES
-- ===============================
CREATE TABLE fact_team_fight_events (
    id SERIAL PRIMARY KEY,
    map_played_id INT REFERENCES maps_played(map_played_id),
    start_time FLOAT,
    end_time FLOAT,
    winning_team_id INT REFERENCES teams(team_id),
    first_death_player_id INT REFERENCES players(player_id),
    --first_elim_player_id INT REFERENCES players(player_id),  TODO
)

CREATE TABLE fact_round_start_event (
    id SERIAL PRIMARY KEY,
    map_played_id INT REFERENCES maps_played(map_played_id),
    round_number INT DEFAULT 1,
    start_time FLOAT
);

CREATE TABLE fact_ult_charge_events (
    id SERIAL PRIMARY KEY,
    map_played_id INT REFERENCES maps_played(map_played_id),
    player_id INT REFERENCES players(player_id),
    hero_id INT REFERENCES heroes(hero_id),
    charge_start_time FLOAT,
    charge_end_time FLOAT,
    charge_duration FLOAT
);

CREATE TABLE fact_ult_usage_events (
    id SERIAL PRIMARY KEY,
    map_played_id INT REFERENCES maps_played(map_played_id),
    player_id INT REFERENCES players(player_id),
    hero_id INT REFERENCES heroes(hero_id),
    ult_ready_time FLOAT,
    ult_used_time FLOAT,
    hold_duration FLOAT
);

CREATE TABLE fact_perk_events (
    id SERIAL PRIMARY KEY,
    map_played_id INT REFERENCES maps_played(map_played_id),
    player_id INT REFERENCES players(player_id),
    hero_id INT REFERENCES heroes(hero_id),
    perk_type TEXT,  -- minor / major
    perk_name TEXT,
    start_time FLOAT,
    end_time FLOAT
);

CREATE TABLE fact_hero_swap_events (
    id SERIAL PRIMARY KEY,
    map_played_id INT REFERENCES maps_played(map_played_id),
    player_id INT REFERENCES players(player_id),
    old_hero_id INT REFERENCES heroes(hero_id),
    new_hero_id INT REFERENCES heroes(hero_id),
    event_time FLOAT
);


-- ===============================
-- INDEXES
-- ===============================
CREATE INDEX idx_players_team_id ON players(current_team_id);
CREATE INDEX idx_matches_date ON matches(date_played);
CREATE INDEX idx_player_team_history_player_id ON player_team_history(player_id);
CREATE INDEX idx_player_team_history_team_id ON player_team_history(team_id);
CREATE INDEX idx_roster_changes_player_id ON roster_changes(player_id);
CREATE INDEX idx_pmhs_player ON player_hero_map_stats(player_id);
CREATE INDEX idx_pmhs_hero ON player_hero_map_stats(hero_id);
CREATE INDEX idx_pmhs_map ON player_hero_map_stats(map_played_id);
CREATE INDEX idx_maps_duration ON maps_played(map_length_seconds);
CREATE INDEX idx_timeseries_map_player_time ON player_timeseries(map_played_id, player_id, time_seconds);

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_players_name_trgm ON players USING gin (LOWER(name) gin_trgm_ops);

-- ===============================
-- VIEWs
-- ===============================
CREATE OR REPLACE VIEW team_rosters AS
SELECT
    t.team_id,
    t.name AS team_name,
    p.player_id,
    p.name AS player_name,
    p.current_elo AS player_elo
FROM players p
JOIN teams t ON p.current_team_id = t.team_id;

CREATE VIEW vw_player_match_stats AS
SELECT
    mp.match_id,
    p.player_id,
    SUM(pms.eliminations) AS eliminations,
    SUM(pms.deaths) AS deaths,
    SUM(pms.assists) AS assists,
    SUM(pms.damage) AS damage,
    SUM(pms.healing) AS healing,
    SUM(pms.mitigated) AS mitigated,
    SUM(pms.fantasy_score) AS fantasy_score
FROM player_map_stats pms
JOIN maps_played mp ON pms.map_played_id = mp.map_played_id
JOIN players p ON p.player_id = pms.player_id
GROUP BY mp.match_id, p.player_id;
