package top.lytree.consul;

import top.lytree.consul.config.ClientConfig;
import top.lytree.consul.monitoring.ClientEventCallback;
import top.lytree.consul.monitoring.ClientEventHandler;
import top.lytree.consul.util.Http;

abstract class BaseClient {

    private final ClientConfig config;
    private final ClientEventHandler eventHandler;
    protected final Http http;

    protected BaseClient(String name, ClientConfig config, ClientEventCallback eventCallback) {
        this.config = config;
        this.eventHandler = new ClientEventHandler(name, eventCallback);
        this.http = new Http(eventHandler);
    }

    public ClientConfig getConfig() {
        return config;
    }

    public ClientEventHandler getEventHandler() { return eventHandler; }
}
