 <b>Twitter Location Analytics</b>
 
A big data project for analysing the location of the tweets using Apache Spark  streaming, Kafka and Cassandra

The project deals with analysing twitter data for following purposes : 

1. Finding out the mood / opinion of the people on certain political issues / products.

2. Analysing the location wise views on any topic products.

The code is in Scala and following technologies are used - 

1. Apache Spark 2.0 
2. Apache Cassandra
3. Apache Kafka - 0.8.2
4. Scala

The project has three parts . 

1. Kafka Producer - Reading twitter stream data, and placing it in the Kafka queues , as the data arrive.
2. Kafka Consumer - Reading from Kafka topic and dumping data in no Sql database(Cassandra)
3. Analysing Data.

<b>To be done</b> - Lambda Architecure needs to be implemented.
