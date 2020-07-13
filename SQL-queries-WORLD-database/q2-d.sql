#d
SELECT country.Name
FROM country RIGHT JOIN countryLanguage ON country.Code = countryLanguage.countryCode
WHERE IsOfficial = 'F'
GROUP BY Name
HAVING COUNT(*) >= 10
ORDER BY COUNT(*) DESC;