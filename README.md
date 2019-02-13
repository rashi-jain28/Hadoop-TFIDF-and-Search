# Hadoop-TFIDF-and-Search
#### Following commands are used for creating the below directory:
```
$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera/rjain12
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera
$ hadoop fs -mkdir /user/cloudera/rjain12/input 
```

All the files(cantrbry corpus) is stored on hdfs in /user/cloudera/rjain12/input

Directories created for the MapReduce programs:

1. /user/cloudera/rjain12/input
2. /user/cloudera/rjain12/output
3. /user/cloudera/rjain12/outputSearch
4. /user/cloudera/rjain12/temp

### DocWordCount.java

DocWordCount file is present in /home/cloudera/Desktop/Programs/

Make sure that there is no /user/cloudera/rjain12/output directory present...

So, the following commands are used to compile the class:
```
$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/Desktop/Programs/DocWordCount.java -d build -Xlint 
```

Jar file is stored in /home/cloudera/docwordcount.jar
```
$ jar -cvf docwordcount.jar -C build/ . 
```

Running the application:
```
$ hadoop jar docwordcount.jar org.myorg.DocWordCount /user/cloudera/rjain12/input /user/cloudera/rjain12/output 
```

output is stored in /user/cloudera/rjain12/output

Checking the output
```
$ hadoop fs -cat /user/cloudera/rjain12/output/
```

Saving the output file using below command:
```
$ hdfs dfs -get /user/cloudera/rjain12/output/part-r-00000 /home/cloudera/Desktop/output/
```


### TermFrequency.java

Remove the output directory first using below command:
```
$ hadoop fs -rm -r /user/cloudera/rjain12/output 
```

TermFrequency file is present in /home/cloudera/Desktop/Programs/
So, the following commands are used to compile the class:
```
$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/Desktop/Programs/TermFrequency.java -d build -Xlint 
```


Jar file is stored in /home/cloudera/termfrequency.jar
```
$ jar -cvf termfrequency.jar -C build/ . 
```

Running the application:
```
$ hadoop jar termfrequency.jar org.myorg.TermFrequency /user/cloudera/rjain12/input /user/cloudera/rjain12/output 
```

output is stored in /user/cloudera/rjain12/output

Checking the output
```
$ hadoop fs -cat /user/cloudera/rjain12/output/*
```

Saving the output file using below command:
```
$ hdfs dfs -get /user/cloudera/rjain12/output/part-r-00000 /home/cloudera/Desktop/output/
```



### TFIDF.java

3 arguments are passed by user
-- input directory
-- temp directory
-- output directory

Remove the output directory first using below command:
```
$ hadoop fs -rm -r /user/cloudera/rjain12/output 
```

Make sure that there is no /user/cloudera/rjain12/temp directory

TFIDF file is present in /home/cloudera/Desktop/Programs/
So, the following commands are used to compile the class:
```
$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/Desktop/Programs/TFIDF.java -d build -Xlint 
```


Jar file is stored in /home/cloudera/tfidf.jar
```
$ jar -cvf tfidf.jar -C build/ . 
```

Running the application:

// temp folder is created which will store the temporary output of first mapreduce job
```
$ hadoop jar tfidf.jar org.myorg.TFIDF /user/cloudera/rjain12/input /user/cloudera/rjain12/temp /user/cloudera/rjain12/output 
```


output is stored in /user/cloudera/rjain12/output

Checking the output
```
$ hadoop fs -cat /user/cloudera/rjain12/output/*
```

Saving the output file using below command:
```
$ hdfs dfs -get /user/cloudera/rjain12/output/part-r-00000 /home/cloudera/Desktop/output/
```

### Search.java


Make sure there is no /user/cloudera/rjain12/outputSearch/
$ hadoop fs -rm -r /user/cloudera/rjain12/outputSearch

3 arguments are passed by user
-- input directory
-- output directory
-- query within quotes ""

Search.java file is present in /home/cloudera/Desktop/Programs/
So, the following commands are used to compile the class:
```
$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/Desktop/Programs/Search.java -d build -Xlint 
```

Jar file is stored in /home/cloudera/search.jar
```
$ jar -cvf search.jar -C build/ . 
```

Running the application:

// The output file which is at /user/cloudera/rjain12/output is used as an input file for Search.java
// and the output is stored at /user/cloudera/rjain12/outputSearch
```
$ hadoop jar search.jar org.myorg.Search /user/cloudera/rjain12/output /user/cloudera/rjain12/outputSearch "computer science"
```


Checking the output
```
$ hadoop fs -cat /user/cloudera/rjain12/outputSearch/*
```

Saving the output file using below command:
```
$ hdfs dfs -get /user/cloudera/rjain12/outputSearch/part-r-00000 /home/cloudera/Desktop/output/
```

Now to run the second query, remove the already existing outputSearch folder
$ hadoop fs -rm -r /user/cloudera/rjain12/outputSearch

Run the below command:
```
$ hadoop jar search.jar org.myorg.Search /user/cloudera/rjain12/output /user/cloudera/rjain12/outputSearch "data analysis"
```

Saving the output file using below command:
```
$ hdfs dfs -get /user/cloudera/rjain12/outputSearch/part-r-00000 /home/cloudera/Desktop/output/
```


