#c-i
SELECT C.Name
FROM country C
WHERE C.Code NOT IN (
    SELECT countryCode
    FROM countryLanguage
    );