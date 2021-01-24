SHELL := /bin/bash

build:
	source queryenv; \
	./build.sh;
db:
	# java RandomDB <tablename> <# of records>
	# java ConvertTxtToTbl <tablename>
	cp testcases/*det .; \
	java RandomDB BILL 5; \
	java RandomDB CART 5; \
	java RandomDB CARTDETAILS 5; \
	java RandomDB CUSTOMER 5; \
	java ConvertTxtToTbl BILL; \
	java ConvertTxtToTbl CART; \
	java ConvertTxtToTbl CARTDETAILS; \
	java ConvertTxtToTbl CUSTOMER; \

clean:
	rm -fv *.md
	rm -fv *.stat
	rm -fv *.tbl
	rm -fv *.txt
	rm -fv *.out
