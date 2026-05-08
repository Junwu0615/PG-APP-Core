SELECT aid % 10 AS machine_id,
       COUNT(*) AS txn_count,
       AVG(abalance) AS avg_balance,
       SUM(abalance) AS total_balance
FROM pgbench_accounts
GROUP BY 1
ORDER BY total_balance DESC;