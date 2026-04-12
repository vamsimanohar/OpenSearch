SELECT userid, searchphrase, COUNT(*) AS c FROM hits GROUP BY userid, searchphrase LIMIT 10;
