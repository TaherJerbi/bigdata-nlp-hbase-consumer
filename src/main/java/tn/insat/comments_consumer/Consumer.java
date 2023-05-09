package tn.insat.comments_consumer;

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
        props.put("partition.assignment.strategy", "range");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Kafka Consumer va souscrire a la liste de topics ici
        consumer.subscribe("reddit-new-comments", "reddit-comments-sentiments");

        // Afficher le nom du topic
        System.out.println("Souscris au topic " + "reddit-new-comments" + " et " + "reddit-comments-sentiments");
        if (consumer == null) {
            System.out.println("----- Consumer null -----");
            return;
        }
        while (true) {
            try {
                Map<String, ConsumerRecords<String, String>> records = consumer.poll(100);
                if (records == null) {
                    System.out.println("----- Records null -----");
                    continue;
                }
                ConsumerRecords<String, String> raw_comments_records = records.get("reddit-new-comments");
                ConsumerRecords<String, String> comments_sentiments_records = records.get("reddit-comments-sentiments");

                // insert raw comments into HBase
                for (ConsumerRecord<String, String> record : raw_comments_records.records()) {
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
                    insertRawComment(record.value());
                }

                // insert comments sentiments into HBase
                for (ConsumerRecord<String, String> record : comments_sentiments_records.records()) {
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
                    insertCommentSentiment(record.value());
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
