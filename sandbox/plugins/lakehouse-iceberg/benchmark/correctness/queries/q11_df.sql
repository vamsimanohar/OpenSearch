SELECT mobilephonemodel, COUNT(DISTINCT userid) AS u FROM hits WHERE mobilephonemodel <> '' GROUP BY mobilephonemodel ORDER BY u DESC LIMIT 10;
