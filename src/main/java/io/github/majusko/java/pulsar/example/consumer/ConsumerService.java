package io.github.majusko.java.pulsar.example.consumer;

import io.github.majusko.java.pulsar.example.configuration.Topics;
import io.github.majusko.java.pulsar.example.data.MyMsg;
import io.github.majusko.pulsar.PulsarMessage;
import io.github.majusko.pulsar.annotation.PulsarConsumer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class ConsumerService {
    public AtomicBoolean stringReceived = new AtomicBoolean(false);
    public AtomicBoolean classReceived = new AtomicBoolean(false);

    @PulsarConsumer(topic = Topics.STRING, clazz = String.class, subscriptionType = SubscriptionType.Key_Shared,
            subscriptionName = "subscription1", consumerName = "consumeString1", initialPosition = SubscriptionInitialPosition.Latest)
    public void consumeString(String message) {
        System.out.println(String.format("接收消息: 内容[%s], 时间[%s]", message, new Date()));
        stringReceived.set(true);
    }

//    @PulsarConsumer(topic = Topics.CLASS, clazz = MyMsg.class, subscriptionType = SubscriptionType.Shared)
//    public void consumeClass(MyMsg message) {
//        System.out.println(message.getData());
//        classReceived.set(true);
//    }

    /**
     * In case you need to access pulsar metadata you simply use PulsarMessage as a wrapper and data will be injected for you.
     * You can configure a topic, consumer and subscription names in application.properties.
     * All failed messages should be handled with Pulsar features like for example "Dead Letter Policies".
     *
     * @param message
     */
    @PulsarConsumer(topic = Topics.CLASS, clazz = MyMsg.class, subscriptionType = SubscriptionType.Shared, subscriptionName = "${my.custom.subscription.name}",
            maxRedeliverCount = 3, deadLetterTopic = "example-class-topic-dlt")
    public void consumeClass(PulsarMessage<MyMsg> message) {
        System.out.println(message.getValue().getData());
        classReceived.set(true);
    }
}
