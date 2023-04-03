package io.github.majusko.java.pulsar.example.consumer;

import io.github.majusko.java.pulsar.example.configuration.Topics;
import io.github.majusko.java.pulsar.example.data.MyMsg;
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

    @PulsarConsumer(topic = Topics.STRING, clazz = String.class, subscriptionType = SubscriptionType.Shared,
            subscriptionName = "subscription1", consumerName = "consumeString1", initialPosition = SubscriptionInitialPosition.Latest)
    public void consumeString(String message) {
        System.out.println(String.format("接收消息: 内容[%s], 时间[%s]", message, new Date()));
        stringReceived.set(true);
    }

    @PulsarConsumer(topic = Topics.CLASS, clazz = MyMsg.class)
    public void consumeClass(MyMsg message) {
        System.out.println(message.getData());
        classReceived.set(true);
    }
}
