#f
SELECT cl.Language
FROM countryLanguage cl RIGHT JOIN country c on c.Code = cl.CountryCode
WHERE c.Population >= 1000000 and cl.IsOfficial = 'T'
GROUP BY Language
ORDER BY SUM(Population) DESC
LIMIT 10;
