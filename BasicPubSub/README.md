## Kafka Basic PubSub
Performs basic pub sub as client. 

#### Start the producer
Parameters:  

1. host
2. topic
```$xslt
java -cp target/BasicPubSub-1.0-SNAPSHOT-jar-with-dependencies.jar com.trace3.KafkaPubSub.Producer localhost test
```

#### Start the consumer
Parameters:

1. host
2. topic
3. group
```$xslt
java -cp target/BasicPubSub-1.0-SNAPSHOT-jar-with-dependencies.jar com.trace3.KafkaPubSub.Consumer localhost test group1
```