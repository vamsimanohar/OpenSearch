SELECT userid, searchphrase, COUNT(*) FROM hits GROUP BY userid, searchphrase ORDER BY COUNT(*) DESC LIMIT 10;
