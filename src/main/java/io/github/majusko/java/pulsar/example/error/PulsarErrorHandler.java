package io.github.majusko.java.pulsar.example.error;

import io.github.majusko.pulsar.consumer.ConsumerAggregator;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

/**
 * All failed messages should be handled with Pulsar features like for example "Dead Letter Policies".
 * However, for debug, development and logging purposes you may want to subscribe to all error messages in your application as well.
 * You just need to autowire ConsumerAggregator and subscribe to onError method.
 */
@Service
public class PulsarErrorHandler {

    private final ConsumerAggregator aggregator;

    public PulsarErrorHandler(ConsumerAggregator aggregator) {
        this.aggregator = aggregator;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void pulsarErrorHandler() {
        aggregator.onError(failedMessage -> failedMessage.getException().printStackTrace());
    }
}
