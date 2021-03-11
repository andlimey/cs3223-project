SHELL := /bin/bash

build:
	source queryenv; \
	./build.sh;

db:
	# java RandomDB <tablename> <# of records>
	# java ConvertTxtToTbl <tablename>
	cp testcases/*det .; \
	java RandomDB BILL 20; \
	java RandomDB CART 5; \
	java RandomDB CARTDETAILS 5; \
	java RandomDB CUSTOMER 20; \
	java ConvertTxtToTbl BILL; \
	java ConvertTxtToTbl CART; \
	java ConvertTxtToTbl CARTDETAILS; \
	java ConvertTxtToTbl CUSTOMER; \

experiment:
	# For experiment
	cp testcases/*det .; \
    java RandomDB AIRCRAFTS 100; \
    java RandomDB CERTIFIED 100; \
    java RandomDB EMPLOYEES 100; \
    java RandomDB FLIGHTS 100; \
    java RandomDB SCHEDULE 100; \
    java ConvertTxtToTbl AIRCRAFTS; \
    java ConvertTxtToTbl CERTIFIED; \
    java ConvertTxtToTbl EMPLOYEES; \
    java ConvertTxtToTbl FLIGHTS; \
    java ConvertTxtToTbl SCHEDULE; \

clean:
	rm -fv *.md
	rm -fv *.stat
	rm -fv *.tbl
	rm -fv *.txt
	rm -fv *.out
	rm -fv *.det
