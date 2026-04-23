SELECT 1 AS "one", url, COUNT(*) AS c FROM hits GROUP BY url ORDER BY c DESC, url LIMIT 10;
