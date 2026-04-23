SELECT clientip, clientip - 1, clientip - 2, clientip - 3, COUNT(*) AS c FROM hits GROUP BY clientip, clientip - 1, clientip - 2, clientip - 3 ORDER BY c DESC, clientip LIMIT 10;
