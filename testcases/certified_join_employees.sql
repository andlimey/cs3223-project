SELECT EMPLOYEES.eid, EMPLOYEES.ename, EMPLOYEES.salary, CERTIFIED.eid, CERTIFIED.aid
FROM EMPLOYEES,CERTIFIED
WHERE CERTIFIED.eid=EMPLOYEES.eid