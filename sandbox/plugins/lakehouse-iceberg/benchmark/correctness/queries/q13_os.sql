SELECT searchphrase, COUNT(*) AS c FROM hits WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10;
