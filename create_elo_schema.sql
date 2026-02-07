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

-- ===============================
-- TEAMS
-- ===============================
CREATE TABLE teams (
    team_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    region VARCHAR(20) NOT NULL,                   -- e.g. 'NA', 'EMEA', 'KR'
    elo FLOAT DEFAULT 1000,
    maps_played INT DEFAULT 0,
    matches_played INT DEFAULT 0,
    active BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ===============================
-- PLAYERS
-- ===============================
CREATE TABLE players (
    player_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    role VARCHAR(10),
    current_team_id INT REFERENCES teams(team_id) ON DELETE SET NULL,
    elo FLOAT DEFAULT 1000,
    maps_played INT DEFAULT 0,
    matches_played INT DEFAULT 0,
    active BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ===============================
-- TOURNAMENTS
-- ===============================
CREATE TABLE tournaments (
    tournament_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(50),
    region VARCHAR(50),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    format TEXT,
    prize_pool FLOAT
);

CREATE TABLE tournament_teams (
    tournament_id INT REFERENCES tournaments(tournament_id),
    team_id INT REFERENCES teams(team_id),
    seed INT,
    final_placement INT
);

-- ===============================
-- MATCHES
-- ===============================
CREATE TABLE matches (
    match_id SERIAL PRIMARY KEY,
    date_played TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    tournament_id INT REFERENCES tournaments(tournament_id),
    match_type VARCHAR(50) NOT NULL,               -- e.g. 'regular', 'regional_playoff', etc.
    game_version VARCHAR(50),                      -- optional: patch or meta version
    first_to INT DEFAULT 3,
    team_a_id INT REFERENCES teams(team_id) ON DELETE CASCADE,
    team_b_id INT REFERENCES teams(team_id) ON DELETE CASCADE,
    winner_id INT REFERENCES teams(team_id) ON DELETE CASCADE,
    loser_id INT REFERENCES teams(team_id) ON DELETE CASCADE,
    score_a INT DEFAULT 0,
    score_b INT DEFAULT 0,
    notes TEXT
);

-- ===============================
-- HEROES
-- ===============================
CREATE TABLE heroes (
    hero_id SERIAL PRIMARY KEY,
    hero_name VARCHAR(50) NOT NULL
);
INSERT INTO heroes (hero_name)
VALUES
('Ana'), ('Ashe'), ('Baptiste'), ('Bastion'), ('Brigitte'), ('Cassidy'), ('D.Va'), ('Doomfist'), ('Echo'), ('Freja'),
('Genji'), ('Hanzo'), ('Hazard'), ('Illari'), ('Junker Queen'), ('Junkrat'), ('Juno'), ('Kiriko'), ('Lifeweaver'),
('Lucio'), ('Mauga'), ('Mei'), ('Mercy'), ('Moira'), ('Orisa'), ('Pharah'), ('Ramattra'), ('Reaper'), ('Reinhardt'),
('Roadhog'), ('Sigma'), ('Sojourn'), ('Soldier: 76'), ('Sombra'), ('Symmetra'), ('Torbjorn'), ('Tracer'), ('Vendetta'),
('Venture'), ('Widowmaker'), ('Winston'), ('Wrecking Ball'), ('Wuyang'), ('Zarya'), ('Zenyatta');

-- ===============================
-- MAP TYPES
-- ===============================
CREATE TABLE map_types (
    map_type_id SERIAL PRIMARY KEY,
    map_type VARCHAR(15)
);
INSERT INTO map_types (map_type)
VALUES
('Clash'), ('Control'), ('Escort'), ('Flashpoint'), ('Hybrid'), ('Push');

-- ===============================
-- MAPS
-- ===============================
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
('Blizzard World', 5), ('Eichenwalde', 5), ('Hollywood', 5), ('King''s Row', 5), ('Midtown', 5), ('Numbani', 5), ('Paraiso', 5),
('Colosseo', 6), ('Esperanca', 6), ('New Queen Street', 6), ('Runasapi', 6);

-- ===============================
-- MATCH_MAPS (each map in a match)
-- ===============================
CREATE TABLE match_maps (
    match_map_id SERIAL PRIMARY KEY,
    match_id INT REFERENCES matches(match_id) ON DELETE CASCADE,
    map_number INT NOT NULL,                       -- 1, 2, 3...
    map_id INT REFERENCES maps(map_id),
    replay_code VARCHAR(10),
    team_a_score INT DEFAULT 0,
    team_b_score INT DEFAULT 0,
    winner_id INT REFERENCES teams(team_id) ON DELETE CASCADE,
    first_ban_team INT REFERENCES teams(team_id) ON DELETE CASCADE,
    first_ban INT REFERENCES heroes(hero_id) ON DELETE CASCADE,
    second_ban INT REFERENCES heroes(hero_id) ON DELETE CASCADE,
    UNIQUE (match_id, map_number)
);

-- ===============================
-- PLAYER MAP STATS
-- ===============================
CREATE TABLE player_map_stats (
    id SERIAL PRIMARY KEY,
    map_played_id INT REFERENCES match_maps(match_map_id) ON DELETE CASCADE,
    player_id INT REFERENCES players(player_id) ON DELETE CASCADE,
    team_id INT REFERENCES teams(team_id) ON DELETE CASCADE,
    fantasy_score FLOAT DEFAULT 0,
    eliminations INT DEFAULT 0,
    deaths INT DEFAULT 0,
    assists INT DEFAULT 0,
    damage INT DEFAULT 0,
    healing INT DEFAULT 0,
    mitigated INT DEFAULT 0
    UNIQUE (match_id, player_id)
);

-- ===============================
-- PLAYER MATCH STATS (aggregated)
-- ===============================
CREATE TABLE player_match_stats (
    stat_id SERIAL PRIMARY KEY,
    match_id INT REFERENCES matches(match_id) ON DELETE CASCADE,
    player_id INT REFERENCES players(player_id) ON DELETE CASCADE,
    team_id INT REFERENCES teams(team_id) ON DELETE CASCADE,
    number_maps_played INT DEFAULT 0,
    fantasy_score FLOAT DEFAULT 0,
    eliminations INT DEFAULT 0,
    deaths INT DEFAULT 0,
    assists INT DEFAULT 0,
    damage INT DEFAULT 0,
    healing INT DEFAULT 0,
    mitigated INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    UNIQUE (match_id, player_id)
);

-- ===============================
-- PLAYER TEAM HISTORY
-- ===============================
CREATE TABLE player_team_history (
    history_id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id) ON DELETE CASCADE,
    team_id INT REFERENCES teams(team_id) ON DELETE CASCADE,
    start_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    end_date TIMESTAMP WITH TIME ZONE,
    UNIQUE (player_id, team_id, start_date)
);

-- ===============================
-- ROSTER CHANGES
-- ===============================
CREATE TABLE roster_changes (
    change_id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id) ON DELETE CASCADE,
    old_team_id INT REFERENCES teams(team_id) ON DELETE SET NULL,
    new_team_id INT REFERENCES teams(team_id) ON DELETE SET NULL,
    change_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    reason TEXT
);

-- ===============================
-- ELO HISTORY
-- ===============================
CREATE TABLE elo_history (
    elo_id SERIAL PRIMARY KEY,
    entity_type VARCHAR(10) NOT NULL,              -- 'team' or 'player'
    entity_id INT NOT NULL,
    old_elo FLOAT NOT NULL,
    new_elo FLOAT NOT NULL,
    delta FLOAT GENERATED ALWAYS AS (new_elo - old_elo) STORED,
    match_id INT REFERENCES matches(match_id) ON DELETE CASCADE,
    roster_change_id INT REFERENCES roster_changes(change_id) ON DELETE CASCADE,
    reason TEXT,                                   -- e.g. 'match result', 'roster change'
    date_recorded TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ===============================
-- PLAYER HERO MAP STATS
-- ===============================
CREATE TABLE player_hero_map_stats (
    id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id) ON DELETE CASCADE,
    match_id INT REFERENCES matches(match_id) ON DELETE CASCADE,
    map_played_id INT REFERENCES match_maps(match_map_id) ON DELETE CASCADE,
    opponent_team_id INT REFERENCES teams(team_id) ON DELETE CASCADE,
    hero_id INT REFERENCES heroes(hero_id) ON DELETE CASCADE,
    seconds_played INT DEFAULT 0,
    eliminations INT DEFAULT 0,
    assists INT DEFAULT 0,
    deaths INT DEFAULT 0,
    damage INT DEFAULT 0,
    healing INT DEFAULT 0,
    mitigated INT DEFAULT 0,
    --UNIQUE (map_played_id, player_id, hero_id)
);



ALTER TABLE elo_history
ADD CONSTRAINT check_one_reason_link
CHECK (
    (match_id IS NOT NULL AND roster_change_id IS NULL) OR
    (match_id IS NULL AND roster_change_id IS NOT NULL)
);

-- ===============================
-- INDEXES
-- ===============================
CREATE INDEX idx_players_team_id ON players(current_team_id);
CREATE INDEX idx_matches_date ON matches(date_played);
CREATE INDEX idx_player_team_history_player_id ON player_team_history(player_id);
CREATE INDEX idx_player_team_history_team_id ON player_team_history(team_id);
CREATE INDEX idx_roster_changes_player_id ON roster_changes(player_id);
CREATE INDEX idx_elo_history_entity ON elo_history(entity_type, entity_id);
CREATE INDEX idx_pmhs_player ON player_hero_map_stats(player_id);
CREATE INDEX idx_pmhs_hero ON player_hero_map_stats(hero_id);
CREATE INDEX idx_pmhs_map ON player_hero_map_stats(map_played_id);
CREATE INDEX idx_pmhs_match ON player_hero_map_stats(match_id);
CREATE INDEX idx_pmhs_opponent ON player_hero_map_stats(opponent_team_id);

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_players_name_trgm ON players USING gin (LOWER(name) gin_trgm_ops);

-- ===============================
-- VIEW: CURRENT TEAM ROSTERS
-- ===============================
CREATE OR REPLACE VIEW team_rosters AS
SELECT
    t.team_id,
    t.name AS team_name,
    p.player_id,
    p.name AS player_name,
    p.elo AS player_elo
FROM players p
JOIN teams t ON p.current_team_id = t.team_id;
