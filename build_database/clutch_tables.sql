CREATE TABLE clutch_bat (
        batter_id VARCHAR NOT NULL,
        year DECIMAL,
        event_count DECIMAL,
        average_win_change DECIMAL NOT NULL,
        center_weighted_win_change DECIMAL NOT NULL
);


CREATE TABLE clutch_pitch (
        pitcher_id VARCHAR NOT NULL,
        year DECIMAL,
        event_count DECIMAL,
        average_win_change DECIMAL NOT NULL,
        center_weighted_win_change DECIMAL NOT NULL
);
