SELECT aid % 10 AS machine_id,
       COUNT(*) AS txn_count,
       AVG(abalance) AS avg_balance
FROM pgbench_accounts
WHERE aid > 49000000
GROUP BY 1;