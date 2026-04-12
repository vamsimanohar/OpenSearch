SELECT mobilephone, mobilephonemodel, COUNT(DISTINCT userid) AS u FROM hits WHERE mobilephonemodel <> '' GROUP BY mobilephone, mobilephonemodel ORDER BY u DESC LIMIT 10;
