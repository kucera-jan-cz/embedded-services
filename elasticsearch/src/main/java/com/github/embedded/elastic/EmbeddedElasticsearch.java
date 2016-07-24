package com.github.embedded.elastic;

import org.elasticsearch.client.Client;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public interface EmbeddedElasticsearch {
    public Client getClient();

    public String getClusterName();

    public void shutdown();

    public void awaitTermination(long timeout, TimeUnit unit);

    public void awaitTermination();

    public void deleteAll();
}
