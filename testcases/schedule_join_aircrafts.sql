SELECT SCHEDULE.flno, SCHEDULE.aid, AIRCRAFTS.aid, AIRCRAFTS.aname, AIRCRAFTS.cruisingrange
FROM SCHEDULE,AIRCRAFTS
WHERE SCHEDULE.aid=AIRCRAFTS.aid