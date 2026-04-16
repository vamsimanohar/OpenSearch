SELECT searchphrase, MIN(url), COUNT(*) AS c FROM hits WHERE url LIKE '%google%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10;
