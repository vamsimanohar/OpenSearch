SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM hits WHERE searchphrase <> '' GROUP BY watchid, clientip ORDER BY c DESC, watchid, clientip LIMIT 10;
