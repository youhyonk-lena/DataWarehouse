#b
SELECT C.Name, C.Continent, C.Population
FROM country C
WHERE C.Name LIKE '%united%' AND C.Population >= 1000000;
