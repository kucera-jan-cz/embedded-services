package com.github.embedded.kafka;

import java.util.concurrent.TimeUnit;

public interface EmbeddedKafka {
    public void shutdown();

    public void awaitTermination(long timeout, TimeUnit unit);

    public void awaitTermination();

    public void deleteAll();
}
