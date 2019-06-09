/*
This class is a work-in-progress. The idea was to poll the results written from Kafka in real-time and
perform unit tests on them.
 */

package poststats.kafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import poststats.datatypes.StatEvent;

import java.util.Collections;
import java.util.Properties;

public class TaskOneOutput {

    public static void main(String[] args){

        /*
        * Poll for Post statistics result from Analytics1.java
        */

        // Poll events using kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9093");
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("group.id", "1");

        // create a Kafka consumer
        KafkaConsumer<Long, StatEvent> resultConsumer = new KafkaConsumer<Long, StatEvent>(kafkaProps,
                new LongDeserializer(),
                new EventDeserializer<>(StatEvent.class)
        );

        resultConsumer.subscribe(Collections.singletonList("resultStream1"));

        try {
            // Infinitely listen to results
            while (true){
                int count = 0;
                ConsumerRecords<Long, StatEvent> records = resultConsumer.poll(0);
                for (ConsumerRecord<Long, StatEvent> record : records)
                {
                    count++;
                    System.out.println(count + " " + record.value());
                }
            }
        }catch(Exception e){
            System.out.println("Exception in result stream: " + e.toString());
        }finally {
            resultConsumer.close();
        }

    }
}
