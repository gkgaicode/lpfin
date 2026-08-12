SELECT 
    c.account_id,
    c.client_name,
    t.trade_id,
    t.trade_amount,
    t.transaction_date,
    SUM(t.trade_amount) OVER (PARTITION BY c.client_name ORDER BY t.transaction_date) as running_total_by_client
FROM lpl_production.gold.clients c
JOIN lpl_production.gold.transactions t 
    ON c.account_id LIKE concat('%', t.account_id, '%')
WHERE DATE(t.transaction_date) >= DATEADD(day, -30, CURRENT_DATE())
  AND c.status = 'ACTIVE';
