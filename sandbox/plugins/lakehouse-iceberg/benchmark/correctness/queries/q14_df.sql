SELECT searchphrase, COUNT(DISTINCT userid) AS u FROM hits WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY u DESC LIMIT 10;
