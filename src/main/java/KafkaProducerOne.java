import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerOne {

    public KafkaProducerOne() throws FileNotFoundException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        File readFile = new File("D:\\NAGP\\BigData\\Final-Assignment_Solution\\Kafka-data-file.txt");

        Scanner scanner = new Scanner(readFile);
        int index = 0;
        while (scanner.hasNextLine()){

            index++;
            Date date = new Date();
            String data = scanner.nextLine();

            ProducerRecord producerRecord = new ProducerRecord("topic-name", date.toString() + "-"+ index, data);

            kafkaProducer.send(producerRecord);
        }

        kafkaProducer.close();


    }
}
