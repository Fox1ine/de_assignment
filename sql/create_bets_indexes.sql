CREATE INDEX IF NOT EXISTS idx_bets_player_id ON bets (player_id);
CREATE INDEX IF NOT EXISTS idx_bets_bet_event ON bets (bet_id, event_id);
