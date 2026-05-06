SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM hits GROUP BY watchid, clientip ORDER BY c DESC, watchid, clientip LIMIT 10;
