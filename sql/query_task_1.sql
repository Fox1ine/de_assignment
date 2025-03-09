WITH filtered_bets AS (
    SELECT *
    FROM bets
    WHERE
        create_time >= '2022-03-14 12:00:00'
        AND event_stage = 'Prematch'
        AND amount >= 10
        AND settlement_time <= '2022-03-15 12:00:00'
        AND bet_type != 'System'
        AND result NOT IN ('Cashout', 'Return','TechnicalReturn')
        AND is_free_bet = FALSE
)
, bet_events AS (
    SELECT
        fb.bet_id,
        fb.player_id,
        COUNT(*) AS total_events,
        COUNT(CASE WHEN e.sport = 'E-Sports' THEN 1 END) AS esports_events,
        MIN(fb.accepted_odd) AS min_odd
    FROM filtered_bets fb
    JOIN events e ON fb.event_id = e.event_id
    GROUP BY fb.bet_id, fb.player_id
)
SELECT DISTINCT player_id  -- Guess unic player_id
FROM bet_events
WHERE
    total_events = esports_events
    AND min_odd >= 1.5;
