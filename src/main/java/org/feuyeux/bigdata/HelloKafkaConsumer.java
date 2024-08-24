package org.feuyeux.bigdata;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class HelloKafkaConsumer {

  private final Consumer<String, String> consumer;
  private final CountDownLatch latch;
  private final String name;
  private final String groupId;

  public HelloKafkaConsumer(
      String bootstrapServers, String groupId, String name, CountDownLatch latch) {
    this.name = name;
    this.groupId = groupId;
    this.latch = latch;
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumer = new KafkaConsumer<>(props);
  }

  public void startConsuming() {
    consumer.subscribe(Collections.singletonList(this.groupId));
    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          log.info("{} received: {}, {}", this.name, record.key(), record.value());
          TimeUnit.SECONDS.sleep(2);
        }
      }
    } catch (InterruptedException e) {
      log.error("", e);
    } finally {
      consumer.close();
      latch.countDown();
    }
  }

  public static void main(String[] args) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(20);
    Executor executor = Executors.newVirtualThreadPerTaskExecutor();
    executor.execute(executeConsume("APPLE", "APPLE_A", latch));
    executor.execute(executeConsume("PEAR", "PEAR_A", latch));
    executor.execute(executeConsume("APPLE", "APPLE_B", latch));
    executor.execute(executeConsume("PEAR", "PEAR_B", latch));
    latch.await();
  }

  private static Runnable executeConsume(String groupId, String nodeId, CountDownLatch latch) {
    return () -> {
      HelloKafkaConsumer consumerExample =
          new HelloKafkaConsumer("127.0.0.1:9092", groupId, nodeId, latch);
      consumerExample.startConsuming();
    };
  }
}
