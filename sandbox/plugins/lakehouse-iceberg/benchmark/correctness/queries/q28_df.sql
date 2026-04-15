SELECT counterid, AVG(length(url)) AS l, COUNT(*) AS c FROM hits WHERE url <> '' GROUP BY counterid HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
