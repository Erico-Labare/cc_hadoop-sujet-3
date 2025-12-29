Setup in hadoop :
````
hdfs dfs -mkdir /input
hdfs dfs -mkdir /lookup

hdfs dfs -put /data/caract-2024.csv /input/
hdfs dfs -put /data/usagers-2024.csv /input/
hdfs dfs -put /data/v_departement_2025.csv /lookup/
````

Current command line to execute in hadoop :
```
hadoop jar /programs/cc_hadoop-sujet-3-1.0-SNAPSHOT-jar-with-dependencies.jar com.cc.hadoop.AccidentDriver /input/caract-2024.csv /input/usagers-2024.csv /lookup_tmp/v_departement_2025.csv /sujet_3_output
```
