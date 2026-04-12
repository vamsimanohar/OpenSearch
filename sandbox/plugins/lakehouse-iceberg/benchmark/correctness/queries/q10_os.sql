SELECT regionid, SUM(advengineid), COUNT(*) AS c, AVG(resolutionwidth), COUNT(DISTINCT userid) FROM hits GROUP BY regionid ORDER BY c DESC LIMIT 10;
