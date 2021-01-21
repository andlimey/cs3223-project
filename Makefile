build:
	bash queryenv;
	./build.sh;
db:
	# java RandomDB <tablename> <# of records>
	# java ConvertTxtToTbl <tablename>
	cd classes; \
	java RandomDB ../testcases/BILL 5; \
	java RandomDB ../testcases/CART 5; \
	java RandomDB ../testcases/CARTDETAILS 5; \
	java RandomDB ../testcases/CUSTOMER 5; \
	java ConvertTxtToTbl ../testcases/BILL; \
	java ConvertTxtToTbl ../testcases/CART; \
	java ConvertTxtToTbl ../testcases/CARTDETAILS; \
	java ConvertTxtToTbl ../testcases/CUSTOMER; \

clean:
	rm -fv testcases/*.md
	rm -fv testcases/*.stat
	rm -fv testcases/*.tbl
	rm -fv testcases/*.txt
