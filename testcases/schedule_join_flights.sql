SELECT FLIGHTS.flno, FLIGHTS.from, FLIGHTS.to, FLIGHTS.distance, FLIGHTS.departs, FLIGHTS.arrives, SCHEDULE.flno, SCHEDULE.aid
FROM FLIGHTS,SCHEDULE
WHERE SCHEDULE.flno=FLIGHTS.flno