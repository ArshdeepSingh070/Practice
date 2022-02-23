import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerOne {

    public Consumer<String, String> createConsumer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers",
                "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        properties.put("key.deserializer",
                StringDeserializer.class.getName());
        properties.put("value.deserializer",
                StringDeserializer.class.getName());

        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(String.valueOf(Collections.singletonList("Topic-name")));

        return consumer;
    }


    public void runConsumer(){

        final Consumer<String, String> consumer = createConsumer();

        final int giveUp = 10;   int noRecordsCount = 0;

        while (true){

            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            consumer.commitAsync();
        }

        consumer.close();
        System.out.println("DONE");
    }
}
