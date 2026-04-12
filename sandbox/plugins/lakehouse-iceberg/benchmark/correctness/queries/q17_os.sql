SELECT userid, searchphrase, COUNT(*) AS c FROM hits GROUP BY userid, searchphrase ORDER BY c DESC LIMIT 10;
