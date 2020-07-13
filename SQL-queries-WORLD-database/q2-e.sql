#e
CREATE VIEW LangCnt AS
    SELECT countryCode, COUNT(Language) AS lng_cnt
    FROM countryLanguage
    GROUP BY countryCode;

SELECT Name
FROM country, LangCnt
WHERE LangCnt.lng_cnt >= ALL (
    SELECT lng_cnt
    FROM LangCnt) AND
      country.Code = LangCnt.CountryCode;