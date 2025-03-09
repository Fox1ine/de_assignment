SELECT
    bet_id,
    event_id,
    odd,
    ROUND(EXP(SUM(LN(odd)) OVER (PARTITION BY bet_id)), 2) AS bet_odd
FROM express_bets;
