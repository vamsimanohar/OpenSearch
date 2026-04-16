SELECT userid, extract(minute FROM to_timestamp_seconds(eventtime)) AS m, searchphrase, COUNT(*) FROM hits GROUP BY userid, m, searchphrase ORDER BY COUNT(*) DESC LIMIT 10;
