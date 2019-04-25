# SparkBatchHbase
This program has function to insert data from CSV to HBASE using Java and Spark.

<br />

## Create Table & Details Table Hbase
###
* Command Shell create table : create 'winevariety', {NAME => 'price', COMPRESSION => 'snappy', REPLICATION_SCOPE => 1}
* table name: winevariety
* row id: variety name of wine (example: 25_Lagunas, Abelis_Carthago)
* column family name: price
* column qualifier name: winery (example: Cabernet_Sauvignon, Chardonnay)
<table>
  <tr>
    <th rowspan="2"></th>
    <th colspan="4">price</th>
  </tr>
  <tr>
    <th>Cabernet_Sauvignon</th>
    <th>Chardonnay</th>
    <th>...</th>
    <th>Red_Blend</th>
  </tr>
  <tr>
    <td>1848_Winery</td>
    <td>25</td>
    <td>25</td>
    <td>...</th>
    <td>45</td>
  </tr>
</table>

### Arguments
args[0]: Tables name
args[1]: Host of zookeeper
args[2]: Host of hbase master
args[2]: Path CSV file

<br />

## How to compile this project
<pre>mvn clean install</pre>

<br />

## How to run the program

<pre>
spark-submit --class com.research.main.BatchHbaseFromCSV --master local[2] /home/reja/SparkBatchHbase/target/SparkBatchHbase-1.0-jar-with-dependencies.jar 'winevariety' 'namenode01.sam.ph' 'namenode01.sam.ph,datanode01.sam.ph,datanode02.sam.ph' '/home/reja/SparkBatchHbase/winemag-data_first150k2.csv'
</pre>