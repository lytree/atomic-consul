package top.lytree.consul;

import top.lytree.consul.config.ClientConfig;
import top.lytree.consul.monitoring.ClientEventCallback;
import top.lytree.consul.monitoring.ClientEventHandler;
import top.lytree.consul.util.Http;

abstract class BaseCacheableClient extends BaseClient {

    private final Consul.NetworkTimeoutConfig networkTimeoutConfig;

    protected BaseCacheableClient(String name, ClientConfig config, ClientEventCallback eventCallback,
                                  Consul.NetworkTimeoutConfig networkTimeoutConfig) {
        super(name, config, eventCallback);
        this.networkTimeoutConfig = networkTimeoutConfig;
    }

    public Consul.NetworkTimeoutConfig getNetworkTimeoutConfig() {
        return networkTimeoutConfig;
    }
}
