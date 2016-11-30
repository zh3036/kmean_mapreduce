default:
	javac -classpath /hadoop/hadoop-0.20.2/hadoop-0.20.2-core.jar -d KMeans_Classes KMeans.java
jar:	
	jar -cvf KMeans.jar -C KMeans_Classes/ .
mkinp:
	hadoop dfs -mkdir /user/hadoop/kmeans/input
rminp:
	hadoop dfs -rmr /user/hadoop/kmeans/input
rmout:
	hadoop dfs -rmr /user/hadoop/kmeans/output
rmdir:
	make rminp
	make rmout
uploadinp:
	hadoop dfs -copyFromLocal data.txt /user/hadoop/kmeans/input/data.txt
	hadoop dfs -copyFromLocal centroid.txt /user/hadoop/kmeans/input/centroid.txt

run:
	hadoop jar KMeans.jar KMeans /user/hadoop/kmeans/input /user/hadoop/kmeans/output 1000 10 1  
showres:
	hadoop dfs -cat /user/hadoop/kmeans/output/*
all:
	make rmdir
	make mkinp
	make 
	make jar
	make uploadinp
	make run
	


