import java.io.FileNotFoundException;

public class Runner {

    public static void main(String[] args) throws FileNotFoundException {

        KafkaProducerOne kafkaProducerOne = new KafkaProducerOne();
        KafkaConsumerOne kafkaConsumerOne = new KafkaConsumerOne();

        kafkaConsumerOne.runConsumer();

    }
}
