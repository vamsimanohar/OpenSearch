SELECT searchengineid, searchphrase, COUNT(*) AS c FROM hits WHERE searchphrase <> '' GROUP BY searchengineid, searchphrase ORDER BY c DESC LIMIT 10;
