DROP TABLE IF EXISTS bets CASCADE;

CREATE TABLE bets (
    id SERIAL PRIMARY KEY,
    bet_id VARCHAR(50) NOT NULL,
    player_id VARCHAR(50) NOT NULL,
    accept_time TIMESTAMP NOT NULL,
    create_time TIMESTAMP NOT NULL,
    settlement_time TIMESTAMP NULL,
    result VARCHAR(50),
    amount NUMERIC(10, 2) NOT NULL,
    payout NUMERIC(10, 2),
    profit NUMERIC(10, 2),
    bet_type VARCHAR(50) NOT NULL,
    bet_size INT NOT NULL,
    accepted_bet_odd NUMERIC(10, 2),
    item_result VARCHAR(50),
    event_stage VARCHAR(50),
    accepted_odd NUMERIC(10, 2),
    item_amount NUMERIC(10, 2),
    item_payout NUMERIC(10, 2),
    item_profit NUMERIC(10, 2),
    event_id VARCHAR(50) NOT NULL,
    is_free_bet BOOLEAN DEFAULT FALSE,
    settlement_status VARCHAR(50)
);
