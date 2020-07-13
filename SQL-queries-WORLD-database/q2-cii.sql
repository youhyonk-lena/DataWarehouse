#c-ii
SELECT country.Name
FROM country LEFT OUTER JOIN countryLanguage ON country.Code = countryLanguage.countryCode
WHERE countryLanguage.Language IS NULL;