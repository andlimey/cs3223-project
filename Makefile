SHELL := /bin/bash

all: clean build db

build:
	source queryenv; \
	./build.sh;

db:
	# java RandomDB <tablename> <# of records>
	# java ConvertTxtToTbl <tablename>
	cp testcases/*det .; \
	java RandomDB BILL 200; \
	java RandomDB CART 200; \
	java RandomDB CARTDETAILS 200; \
	java RandomDB CUSTOMER 200; \
	java ConvertTxtToTbl BILL; \
	java ConvertTxtToTbl CART; \
	java ConvertTxtToTbl CARTDETAILS; \
	java ConvertTxtToTbl CUSTOMER; \

experiment:
	# For experiment
	cp testcases/*det .; \
    java RandomDB AIRCRAFTS 10000; \
    java RandomDB CERTIFIED 10000; \
    java RandomDB EMPLOYEES 10000; \
    java RandomDB FLIGHTS 10000; \
    java RandomDB SCHEDULE 10000; \
    java ConvertTxtToTbl AIRCRAFTS; \
    java ConvertTxtToTbl CERTIFIED; \
    java ConvertTxtToTbl EMPLOYEES; \
    java ConvertTxtToTbl FLIGHTS; \
    java ConvertTxtToTbl SCHEDULE; \

clean:
	mv README.md README.temp
	rm -fv *.md
	rm -fv *.stat
	rm -fv *.tbl
	rm -fv *.txt
	rm -fv *.out
	rm -fv *.det
	rm -fv temp-*
	mv README.temp README.md
