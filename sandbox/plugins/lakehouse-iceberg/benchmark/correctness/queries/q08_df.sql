SELECT advengineid, COUNT(*) FROM hits WHERE advengineid <> 0 GROUP BY advengineid ORDER BY COUNT(*) DESC;
