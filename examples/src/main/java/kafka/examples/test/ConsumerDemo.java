package kafka.examples.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        String topicName = "test-topic";
        String groupId = "test-group";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "groupId");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.ineterval.ms", "1000");
        // 每次重启都是从最早的offset开始读取，不是接着上一次
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000); // 超时时间
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(123123);
                    System.out.println(record.offset() + ", " + record.key() + ", " + record.value());
                }
            }
        } catch (Exception e) {

        }
    }
}