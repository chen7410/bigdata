use airlineDataset
-- Q1
SELECT airportId, name FROM AIRPORT WHERE country = 'India' ORDER BY airportId DESC;

-- Q2
SELECT DISTINCT AIRLINE.airlineId, name FROM ROUTE 
JOIN AIRLINE ON ROUTE.airlineId = AIRLINE.airlineId WHERE stops = 0;

-- Q3
SELECT DISTINCT AIRLINE.airlineId, name FROM ROUTE 
JOIN AIRLINE ON ROUTE.airlineId = AIRLINE.airlineId WHERE codeshare = 'Y';

-- Q4
SELECT * FROM AIRPORT WHERE altitude = (SELECT MAX(altitude) FROM AIRPORT);

-- Q5
SELECT DISTINCT airlineId, name, iata, active FROM AIRLINE 
WHERE active = '1' and iata LIKE '%[A-Za-z][A-Za-z]%' and country = 'united states';

SELECT * FROM AIRLINE WHERE airlineId=16135
