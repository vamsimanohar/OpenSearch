SELECT advengineid, COUNT(*) AS c FROM hits WHERE advengineid <> 0 GROUP BY advengineid ORDER BY c DESC;
