package io.github.majusko.java.pulsar.example;

import io.github.majusko.java.pulsar.example.configuration.Topics;
import io.github.majusko.java.pulsar.example.producer.ProducerService;
import io.github.majusko.pulsar.producer.PulsarTemplate;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * pulsar-java-spring-boot-starter使用参考 https://github.com/majusko/pulsar-java-spring-boot-starter
 */
@SpringBootApplication
public class JavaPulsarExampleApplication {

    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(JavaPulsarExampleApplication.class, args);

        ProducerService producerService = applicationContext.getBean(ProducerService.class);
        send1(producerService);

//        PulsarTemplate<String> pulsarTemplate = applicationContext.getBean(PulsarTemplate.class);
//        send2(pulsarTemplate);
//        send3(pulsarTemplate);
    }

    public static void send1(ProducerService producerService) {
        try {
            producerService.sendStringMsg();
            producerService.sendClassMsg();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    public static void send2(PulsarTemplate<String> pulsarTemplate) {
        try {
            // 同步发送消息
            MessageId messageId = pulsarTemplate.send(Topics.STRING, "同步发送消息");

            // 异步发送消息
            CompletableFuture<MessageId> future = pulsarTemplate.sendAsync(Topics.STRING, "异步发送消息");
            MessageId messageId1 = future.get();

            // 自定义消息发送
            String value = "自定义消息发送";
            TypedMessageBuilder<String> builder = pulsarTemplate.createMessage(Topics.STRING, value);
            builder.key("k1")
                    .orderingKey("k1".getBytes())
                    .property("n1", "v1")
                    // Only shared (include key-shared) subscriptions support delayed message delivery.
                    // In other subscriptions, delayed messages are dispatched immediately.
                    .deliverAfter(1, TimeUnit.MINUTES)
                    .send();
            System.out.println(String.format("发送消息: 内容[%s], 时间[%s]", value, new Date()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void send3(PulsarTemplate<String> pulsarTemplate) {
        try {
            // 批量发送消息，测试订阅类型
            for (int i = 0; i < 5; i++) {
                String key = "k" + i;
                String orderingKey = "ok" + i;
                for (int j = 0; j < 2; j++) {
                    String value = key + "-" + "v" + j;
                    TypedMessageBuilder<String> builder = pulsarTemplate.createMessage(Topics.STRING, value);
                    builder.key(key)
                            .orderingKey(orderingKey.getBytes())
                            .send();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
