SELECT searchphrase FROM hits WHERE searchphrase <> '' ORDER BY eventtime, searchphrase LIMIT 10;
