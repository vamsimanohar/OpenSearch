SELECT userid, EXTRACT(MINUTE FROM eventtime) AS m, searchphrase, COUNT(*) AS c FROM hits GROUP BY userid, EXTRACT(MINUTE FROM eventtime), searchphrase ORDER BY c DESC LIMIT 10;
