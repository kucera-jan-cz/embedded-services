package com.github.embedded.zookeeper;

import java.util.concurrent.TimeUnit;

public interface EmbeddedZookeeper {
    public String getConnectionString();

    public void shutdown();

    public void awaitTermination(long timeout, TimeUnit unit);

    public void awaitTermination();
}
