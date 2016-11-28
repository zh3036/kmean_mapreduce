default:
	javac -classpath /hadoop/hadoop-0.20.2/hadoop-0.20.2-core.jar -d WordCount_Classes WordCount.java
jar:	
	jar -cvf WordCount.jar -C WordCount_Classes/ .
mkinp:
	hadoop dfs -mkdir /user/hadoop/wordcount/input
rminp:
	hadoop dfs -rmr /user/hadoop/wordcount/input
rmout:
	hadoop dfs -rmr /user/hadoop/wordcount/output
rmdir:
	make rminp
	make rmout
uploadinp:
	hadoop dfs -copyFromLocal file01 /user/hadoop/wordcount/input
run:
	hadoop jar WordCount.jar WordCount /user/hadoop/wordcount/input /user/hadoop/wordcount/output
showres:
	hadoop dfs -cat /user/hadoop/wordcount/output/*
all:
	make rmdir
	make mkinp
	make 
	make jar
	make uploadinp
	make run
	


