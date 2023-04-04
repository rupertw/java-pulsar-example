package io.github.majusko.java.pulsar.example.interceptor;

import io.github.majusko.pulsar.consumer.DefaultConsumerInterceptor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.springframework.stereotype.Component;

@Component
public class PulsarConsumerInterceptor extends DefaultConsumerInterceptor<Object> {

    @Override
    public Message<Object> beforeConsume(Consumer<Object> consumer, Message<Object> message) {
        return super.beforeConsume(consumer, message);
    }

}