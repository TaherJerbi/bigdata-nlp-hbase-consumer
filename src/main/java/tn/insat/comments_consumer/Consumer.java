package tn.insat.comments_consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
public class Consumer {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Kafka Consumer va souscrire a la liste de topics ici
        consumer.subscribe(Arrays.asList("reddit-new-comments", "reddit-comments-sentiments"));

        // Afficher le nom du topic
        System.out.println("Souscris au topic " + "reddit-new-comments" + " et " + "reddit-comments-sentiments");
        if (consumer == null) {
            System.out.println("----- Consumer null -----");
            return;
        }
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
                System.out.println("Nombre de records: "+records.count());
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());

                    if (record.topic().equals("reddit-new-comments")) {
                         insertRawComment(record.value());
                    } else if (record.topic().equals("reddit-comments-sentiments")) {
                         insertCommentSentiment(record.value());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static void insertRawComment(String value) {
        System.out.println("---");
        System.out.println("Inserting raw comment: "+value);

        HbaseRepository hbaseRepository = HbaseRepository.getInstance();

        hbaseRepository.insertRawComment(value);

        System.out.println("Inserted raw comment");
        System.out.println("---");
    }

    static void insertCommentSentiment(String value) {
        System.out.println("---");
        System.out.println("Inserting comment sentiment: "+value);

        HbaseRepository hbaseRepository = HbaseRepository.getInstance();

        hbaseRepository.insertCommentSentiment(value);

        System.out.println("Inserted comment sentiment");
        System.out.println("---");
    }
}
