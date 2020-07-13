#a
SELECT city.Name, city.Population
FROM city RIGHT JOIN country on city.CountryCode = country.Code
WHERE country.Name = 'United States'
ORDER BY city.Population DESC LIMIT 10;