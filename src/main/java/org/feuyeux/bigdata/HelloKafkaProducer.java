package org.feuyeux.bigdata;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

@Slf4j
public class HelloKafkaProducer {
  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();
    props.put("bootstrap.servers", "127.0.0.1:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(props);

    for (int i = 0; i < 100; i++) {
      TimeUnit.MILLISECONDS.sleep(200);
      sendMessage(producer, "APPLE", "hello", String.valueOf(i));
      sendMessage(producer, "PEAR", "bonjour", String.valueOf(i));
    }
    producer.close();
  }

  private static void sendMessage(
      Producer<String, String> producer, String topic, String key, String message) {
    producer.send(
        new ProducerRecord<>(topic, key, message),
        (metadata, exception) -> {
          if (exception != null) {
            log.error("Failed to send message", exception);
          } else {
            log.info(
                "Sent to topic {} partition [{}] offset {}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset());
          }
        });
  }
}
