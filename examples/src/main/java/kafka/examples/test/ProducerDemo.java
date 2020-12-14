package kafka.examples.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemo {
	
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();

		// 这里可以配置几台broker即可，他会自动从broker去拉取元数据进行缓存
		props.put("bootstrap.servers", "localhost:9092");
		// 这个就是负责把发送的key从字符串序列化为字节数组
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 这个就是负责把你发送的实际的message从字符串序列化为字节数组
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "-1");
		props.put("retries", 3);
		props.put("batch.size", 323840);
		props.put("linger.ms", 10);
		props.put("buffer.memory", 33554432);
		props.put("max.block.ms", 30000);
		
		// 创建一个Producer实例：线程资源，跟各个broker建立socket连接资源
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		ProducerRecord<String, String> record = new ProducerRecord<>(
				"test-topic-1", "test-key", "test-value");
		
		// 这是异步发送的模式
//		producer.send(record, new Callback() {
//
//			@Override
//			public void onCompletion(RecordMetadata metadata, Exception exception) {
//				if(exception == null) {
//					// 消息发送成功
//					System.out.println("消息发送成功");
//				} else {
//					// 消息发送失败，需要重新发送
//				}
//			}
//
//		});
//
//		Thread.sleep(10 * 1000);
//
		// 这是同步发送的模式
		producer.send(record).get();
		// 你要一直等待人家后续一系列的步骤都做完，发送消息之后
		// 有了消息的回应返回给你，你这个方法才会退出来

		producer.close();
	}
	
}
