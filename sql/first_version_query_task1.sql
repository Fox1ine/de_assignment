---- the first option of the request, which takes longer
WITH bets_events AS (
    SELECT *
    FROM bets b
    INNER JOIN events e USING (event_id)
),
bets_events_filtered AS (
    SELECT *
    FROM bets_events
    WHERE
        accept_time >= TIMESTAMP '2022-03-14 12:00:00'
        AND settlement_time <= TIMESTAMP '2022-03-15 12:00:00'
        AND event_stage = 'Prematch'
        AND bet_type IN ('Ordinar', 'Express')
        AND amount >= 10
        AND accepted_odd >= 1.5
        AND item_result NOT IN ('Cashout', 'Return', 'TechnicalReturn')
        AND is_free_bet = FALSE
),
ordinar_esports AS (
    SELECT DISTINCT player_id
    FROM bets_events_filtered
    WHERE sport = 'E-Sports' AND bet_type = 'Ordinar'
),
invalid_express_bets AS (
    SELECT DISTINCT b.bet_id
    FROM bets b
    JOIN events e USING (event_id)
    WHERE NOT (
        sport = 'E-Sports'
        AND bet_type = 'Express'
    )
),
valid_express_bets AS (
    SELECT bet_id
    FROM bets_events_filtered
    WHERE bet_type = 'Express'
      AND bet_id NOT IN (SELECT bet_id FROM invalid_express_bets)
    GROUP BY bet_id
    HAVING MIN(accepted_odd) >= 1.5
),
express_players AS (
    SELECT DISTINCT player_id
    FROM bets_events_filtered
    WHERE bet_id IN (SELECT bet_id FROM valid_express_bets)
)
SELECT player_id
FROM (
    SELECT player_id FROM ordinar_esports
    UNION
    SELECT player_id FROM express_players
) AS final_players;
