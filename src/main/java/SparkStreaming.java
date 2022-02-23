import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SparkStreaming {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(1000));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton("topic-name");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(jsc, String.class,
                String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);


        AtomicInteger ai = new AtomicInteger();
        AtomicReference<AtomicInteger> ar = new AtomicReference<>(new AtomicInteger());

        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("RDD ==" + rdd);
            System.out.println("RDD COUNT ==" + rdd.count());

            if (rdd.count() > 0) {
                List<Tuple2<String, String>> text = rdd.collect();
                rdd.collect().forEach(rawRecord -> {
                    System.out.println(rawRecord._2);
                    ai.addAndGet(rawRecord._2.split(" ").length);
                    ar.set(ai);
                });
                System.out.println("Total count is " + ar);

            }
        });

        jsc.start();
        jsc.awaitTermination();


    }
}
