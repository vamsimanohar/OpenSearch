SELECT userid, searchphrase, COUNT(*) FROM hits GROUP BY userid, searchphrase ORDER BY userid, searchphrase LIMIT 10;
