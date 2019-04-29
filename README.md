# SparkBatchHbase
This program has function to insert data from CSV to HBASE using Java and Spark.

<br />

## Create Table & Details Table Hbase
###
* Command Shell create table : create 'restaurant', {NAME => 'rating', COMPRESSION => 'snappy', REPLICATION_SCOPE => 1}
* table name: restaurant
* row id: variety name of city (example: Abu_Dhabi, Agra)
* column family name: price
* column qualifier name: restaurant (example: Pizza_Di_Rocco, Tikka_Tonight)
<table>
  <tr>
    <th rowspan="2"></th>
    <th colspan="4">price</th>
  </tr>
  <tr>
    <th>Pizza_Di_Rocco</th>
    <th>Tikka_Tonight</th>
    <th>...</th>
    <th>Tamba</th>
  </tr>
  <tr>
    <td>1848_Winery</td>
    <td>4.4</td>
    <td>4</td>
    <td>...</th>
    <td>4.7</td>
  </tr>
</table>

### Arguments
- args[0]: Tables name
- args[1]: Host of zookeeper
- args[2]: Host of hbase master
- args[3]: Path CSV file

<br />

## How to compile this project
<pre>mvn clean install</pre>

<br />

## How to run the program

<pre>
spark-submit --class com.research.main.BatchHbaseFromCSV --master local[2] /home/reja/SparkBatchHbase/target/SparkBatchHbase-1.0-jar-with-dependencies.jar 'restaurant' 'namenode01.sam.ph' 'namenode01.sam.ph,datanode01.sam.ph,datanode02.sam.ph' '/home/reja/SparkBatchHbase/zomato.csv'
</pre>