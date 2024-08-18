package top.lytree.consul;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import javax.net.ssl.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;
import okhttp3.*;
import okhttp3.internal.http.HttpHeaders;
import okio.Buffer;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.lytree.consul.cache.TimeoutInterceptor;
import top.lytree.consul.config.ClientConfig;
import top.lytree.consul.monitoring.ClientEventCallback;
import top.lytree.consul.util.Jackson;
import top.lytree.consul.util.TrustManagerUtils;
import top.lytree.consul.util.bookend.ConsulBookend;
import top.lytree.consul.util.bookend.ConsulBookendInterceptor;
import top.lytree.consul.util.failover.ConsulFailoverInterceptor;
import top.lytree.consul.util.failover.strategy.ConsulFailoverStrategy;

import okhttp3.internal.Util;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

/**
 * Client for interacting with the Consul HTTP API.
 *
 * @author rfast
 */
public class Consul {

    /**
     * Default Consul HTTP API host.
     */
    public static final String DEFAULT_HTTP_HOST = "localhost";

    /**
     * Default Consul HTTP API port.
     */
    public static final int DEFAULT_HTTP_PORT = 8500;

    private final AgentClient agentClient;
    private final AclClient aclClient;
    private final HealthClient healthClient;
    private final KeyValueClient keyValueClient;
    private final CatalogClient catalogClient;
    private final StatusClient statusClient;
    private final SessionClient sessionClient;
    private final EventClient eventClient;
    private final PreparedQueryClient preparedQueryClient;
    private final CoordinateClient coordinateClient;
    private final OperatorClient operatorClient;
    private final SnapshotClient snapshotClient;

    private final ExecutorService executorService;
    private final ConnectionPool connectionPool;
    private final OkHttpClient okHttpClient;
    private boolean destroyed;

    /**
     * Package-private constructor.
     *
     * @param agentClient         the {@link AgentClient}
     * @param healthClient        the {@link HealthClient}
     * @param keyValueClient      the {@link KeyValueClient}
     * @param catalogClient       the {@link CatalogClient}
     * @param statusClient        the {@link StatusClient}
     * @param sessionClient       the {@link SessionClient}
     * @param eventClient         the {@link EventClient}
     * @param preparedQueryClient the {@link PreparedQueryClient}
     * @param coordinateClient    the {@link CoordinateClient}
     * @param operatorClient      the {@link OperatorClient}
     * @param executorService     the executor service provided to OkHttp
     * @param connectionPool      the OkHttp connection pool
     * @param aclClient           the {@link AclClient}
     * @param snapshotClient      the {@link SnapshotClient}
     * @param okHttpClient        the {@link OkHttpClient}
     */
    protected Consul(AgentClient agentClient, HealthClient healthClient,
                     KeyValueClient keyValueClient, CatalogClient catalogClient,
                     StatusClient statusClient, SessionClient sessionClient,
                     EventClient eventClient, PreparedQueryClient preparedQueryClient,
                     CoordinateClient coordinateClient, OperatorClient operatorClient,
                     ExecutorService executorService, ConnectionPool connectionPool,
                     AclClient aclClient, SnapshotClient snapshotClient,
                     OkHttpClient okHttpClient) {
        this.agentClient = agentClient;
        this.healthClient = healthClient;
        this.keyValueClient = keyValueClient;
        this.catalogClient = catalogClient;
        this.statusClient = statusClient;
        this.sessionClient = sessionClient;
        this.eventClient = eventClient;
        this.preparedQueryClient = preparedQueryClient;
        this.coordinateClient = coordinateClient;
        this.operatorClient = operatorClient;
        this.executorService = executorService;
        this.connectionPool = connectionPool;
        this.aclClient = aclClient;
        this.snapshotClient = snapshotClient;
        this.okHttpClient = okHttpClient;
    }

    /**
     * Destroys the Object internal state.
     */
    public void destroy() {
        this.destroyed = true;
        this.okHttpClient.dispatcher().cancelAll();
        this.executorService.shutdownNow();
        this.connectionPool.evictAll();
    }

    /**
     * Check whether the internal state has been shut down.
     *
     * @return true if {@link #destroy()} was called, otherwise false
     */
    public boolean isDestroyed() {
        return destroyed;
    }

    /**
    * Get the Agent HTTP client.
    * <p>
    * /v1/agent
    *
    * @return The Agent HTTP client.
    */
    public AgentClient agentClient() {
        return agentClient;
    }

    /**
     * Get the ACL HTTP client.
     * <p>
     * /v1/acl
     *
     * @return The ACL HTTP client.
     */
    public AclClient aclClient() {
        return aclClient;
    }

    /**
     * Get the Catalog HTTP client.
     * <p>
     * /v1/catalog
     *
     * @return The Catalog HTTP client.
     */
    public CatalogClient catalogClient() {
        return catalogClient;
    }

    /**
     * Get the Health HTTP client.
     * <p>
     * /v1/health
     *
     * @return The Health HTTP client.
     */
    public HealthClient healthClient() {
        return healthClient;
    }

    /**
     * Get the Key/Value HTTP client.
     * <p>
     * /v1/kv
     *
     * @return The Key/Value HTTP client.
     */
    public KeyValueClient keyValueClient() {
        return keyValueClient;
    }

    /**
     * Get the Status HTTP client.
     * <p>
     * /v1/status
     *
     * @return The Status HTTP client.
     */
    public StatusClient statusClient() {
        return statusClient;
    }

    /**
     * Get the SessionInfo HTTP client.
     * <p>
     * /v1/session
     *
     * @return The SessionInfo HTTP client.
     */
    public SessionClient sessionClient() {
        return sessionClient;
    }

    /**
     * Get the Event HTTP client.
     * <p>
     * /v1/event
     *
     * @return The Event HTTP client.
     */
    public EventClient eventClient() {
        return eventClient;
    }

    /**
     * Get the Prepared Query HTTP client.
     * <p>
     * /v1/query
     *
     * @return The Prepared Query HTTP client.
     */
    public PreparedQueryClient preparedQueryClient() {
        return preparedQueryClient;
    }

    /**
     * Get the Coordinate HTTP client.
     * <p>
     * /v1/coordinate
     *
     * @return The Coordinate HTTP client.
     */
    public CoordinateClient coordinateClient() {
        return coordinateClient;
    }

    /**
     * Get the Operator HTTP client.
     * <p>
     * /v1/operator
     *
     * @return The Operator HTTP client.
     */
    public OperatorClient operatorClient() {
        return operatorClient;
    }

    /**
     * Get the Snapshot HTTP client.
     * <p>
     * /v1/snapshot
     *
     * @return The Snapshot HTTP client.
     */
    public SnapshotClient snapshotClient() {
        return snapshotClient;
    }

    /**
     * Creates a new {@link Builder} object.
     *
     * @return A new Consul builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Used to create a default Consul client.
     *
     * @return A default {@link Consul} client.
     */
    @VisibleForTesting
    public static Consul newClient() {
        return builder().build();
    }

    /**
     * Builder for {@link Consul} client objects.
     */
    public static class Builder {
        private String scheme = "http";
        private URL url;
        private SSLContext sslContext;
        private X509TrustManager trustManager;
        private HostnameVerifier hostnameVerifier;
        private Proxy proxy;
        private boolean ping = true;
        private Interceptor authInterceptor;
        private Interceptor aclTokenInterceptor;
        private Interceptor headerInterceptor;
        private Interceptor consulBookendInterceptor;
        private Interceptor consulFailoverInterceptor;
        private final NetworkTimeoutConfig.Builder networkTimeoutConfigBuilder = new NetworkTimeoutConfig.Builder();
        private ExecutorService executorService;
        private ConnectionPool connectionPool;
        private ClientConfig clientConfig;
        private ClientEventCallback clientEventCallback;

        {
            try {
                url = new URL(scheme, DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT, "");
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Constructs a new builder.
         */
        Builder() {

        }

        /**
         * Sets the URL from a {@link URL} object.
         *
         * @param url The Consul agent URL.
         * @return The builder.
         */
        public Builder withUrl(URL url) {
            this.url = url;

            return this;
        }

        /**
         * Use HTTPS connections for all requests.
         *
         * @param withHttps Set to true to use https for all Consul requests.
         * @return The builder.
         */
        public Builder withHttps(boolean withHttps) {
            if (withHttps) {
                this.scheme = "https";
            } else {
                this.scheme = "http";
            }

            //if url was already generated from a call to withMultipleHostAndPort or withMultipleHostAndPort
            //it might have the old scheme saved into url, so recreate it here if it has changed
            if (!this.url.getProtocol().equals(this.scheme)) {
                try {
                    this.url = new URL(scheme, this.url.getHost(), this.url.getPort(), "");
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
            }
            return this;
        }

        /**
         * Instructs the builder that the AgentClient should attempt a ping before returning the Consul instance
         *
         * @param ping Whether the ping should be done or not
         * @return The builder.
         */
        public Builder withPing(boolean ping) {
            this.ping = ping;

            return this;
        }

        /**
         * Sets the username and password to be used for basic authentication
         *
         * @param username the value of the username
         * @param password the value of the password
         * @return The builder.
         */
        public Builder withBasicAuth(String username, String password) {
            String credentials = username + ":" + password;
            final String basic = "Basic " + BaseEncoding.base64().encode(credentials.getBytes());
            authInterceptor = chain -> {
                Request original = chain.request();

                Request.Builder requestBuilder = original.newBuilder()
                        .header("Authorization", basic)
                        .method(original.method(), original.body());

                Request request = requestBuilder.build();
                return chain.proceed(request);
            };

            return this;
        }

        /**
         * Sets the token used for authentication
         *
         * @param token the token
         * @return The builder.
         */
        public Builder withTokenAuth(String token) {
            authInterceptor = chain -> {
                Request original = chain.request();

                Request.Builder requestBuilder = original.newBuilder()
                        .header("X-Consul-Token", token)
                        .method(original.method(), original.body());

                Request request = requestBuilder.build();
                return chain.proceed(request);
            };

            return this;
        }

        /**
         * Sets the ACL token to be used with Consul
         *
         * @param token the value of the token
         * @return The builder.
         */
        public Builder withAclToken(final String token) {
            aclTokenInterceptor = chain -> {
                Request original = chain.request();

                HttpUrl originalUrl = original.url();
                String rewrittenUrl;
                if (originalUrl.queryParameterNames().isEmpty()) {
                    rewrittenUrl = originalUrl.url().toExternalForm() + "?token=" + token;
                } else {
                    rewrittenUrl = originalUrl.url().toExternalForm() + "&token=" + token;
                }

                Request.Builder requestBuilder = original.newBuilder()
                        .url(rewrittenUrl)
                        .method(original.method(), original.body());

                Request request = requestBuilder.build();
                return chain.proceed(request);
            };

            return this;
        }

        /**
         * Sets headers to be included with each Consul request.
         *
         * @param headers Map of headers.
         * @return The builder.
         */
        public Builder withHeaders(final Map<String, String> headers) {
            headerInterceptor = chain -> {
                Request.Builder requestBuilder = chain.request().newBuilder();

                for (Map.Entry<String, String> header : headers.entrySet()) {
                    requestBuilder.addHeader(header.getKey(), header.getValue());
                }

                return chain.proceed(requestBuilder.build());
            };

            return this;
        }

        /**
         * Attaches a {@link ConsulBookend} to each Consul request. This can be used for gathering
         * metrics timings or debugging. {@see ConsulBookend}
         *
         * @param consulBookend The bookend implementation.
         * @return The builder.
         */
        public Builder withConsulBookend(ConsulBookend consulBookend) {
            consulBookendInterceptor = new ConsulBookendInterceptor(consulBookend);

            return this;
        }

        /**
         * Sets the URL from a {@link HostAndPort} object.
         *
         * @param hostAndPort The Consul agent host and port.
         * @return The builder.
         */
        public Builder withHostAndPort(HostAndPort hostAndPort) {
            try {
                this.url = new URL(scheme, hostAndPort.getHost(), hostAndPort.getPort(), "");
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }

            return this;
        }

        /**
         * Sets the list of hosts to contact if the current request target is
         * unavailable. When the call to a particular URL fails for any reason, the next {@link HostAndPort} specified
         * is used to retry the request. This will continue until all urls are exhuasted.
         *
         * @param hostAndPort           A collection of {@link HostAndPort} that define the list of Consul agent addresses to use.
         * @param blacklistTimeInMillis The timeout (in milliseconds) to blacklist a particular {@link HostAndPort} before trying to use it again.
         * @return The builder.
         */
        public Builder withMultipleHostAndPort(Collection<HostAndPort> hostAndPort, long blacklistTimeInMillis) {
            Preconditions.checkArgument(blacklistTimeInMillis >= 0, "Negative Value");
            Preconditions.checkArgument(hostAndPort.size() >= 2, "Minimum of 2 addresses are required");

            consulFailoverInterceptor = new ConsulFailoverInterceptor(hostAndPort, blacklistTimeInMillis);
            withHostAndPort(hostAndPort.stream().findFirst().get());

            return this;
        }

        /**
         * Constructs a failover interceptor with the given {@link ConsulFailoverStrategy}.
         *
         * @param strategy The strategy to use.
         * @return The builder.
         */
        public Builder withFailoverInterceptor(ConsulFailoverStrategy strategy) {
            Preconditions.checkArgument(strategy != null, "Must not provide a null strategy");

            consulFailoverInterceptor = new ConsulFailoverInterceptor(strategy);
            return this;
        }

        /**
         * Sets the URL from a string.
         *
         * @param url The Consul agent URL.
         * @return The builder.
         */
        public Builder withUrl(String url) {
            try {
                this.url = new URL(url);
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }

            return this;
        }

        /**
         * Sets the {@link SSLContext} for the client.
         *
         * @param sslContext The SSL context for HTTPS agents.
         * @return The builder.
         */
        public Builder withSslContext(SSLContext sslContext) {
            this.sslContext = sslContext;

            return this;
        }

        /**
         * Sets the {@link X509TrustManager} for the client.
         *
         * @param trustManager The SSL trust manager for HTTPS agents.
         * @return The builder.
         */
        public Builder withTrustManager(X509TrustManager trustManager) {
            this.trustManager = trustManager;

            return this;
        }

        /**
         * Sets the {@link HostnameVerifier} for the client.
         *
         * @param hostnameVerifier The hostname verifier to use.
         * @return The builder.
         */
        public Builder withHostnameVerifier(HostnameVerifier hostnameVerifier) {
            this.hostnameVerifier = hostnameVerifier;

            return this;
        }

        /**
         * Sets the {@link Proxy} for the client.
         *
         * @param proxy The proxy to use.
         * @return The builder
         */
        public Builder withProxy(Proxy proxy) {
            this.proxy = proxy;

            return this;
        }

        /**
         * Connect timeout for OkHttpClient
         *
         * @param timeoutMillis timeout values in milliseconds
         * @return The builder
         */
        public Builder withConnectTimeoutMillis(long timeoutMillis) {
            Preconditions.checkArgument(timeoutMillis >= 0, "Negative value");
            this.networkTimeoutConfigBuilder.withConnectTimeout((int) timeoutMillis);
            return this;
        }

        /**
         * Read timeout for OkHttpClient
         *
         * @param timeoutMillis timeout value in milliseconds
         * @return The builder
         */
        public Builder withReadTimeoutMillis(long timeoutMillis) {
            Preconditions.checkArgument(timeoutMillis >= 0, "Negative value");
            this.networkTimeoutConfigBuilder.withReadTimeout((int) timeoutMillis);

            return this;
        }

        /**
         * Write timeout for OkHttpClient
         *
         * @param timeoutMillis timeout value in milliseconds
         * @return The builder
         */
        public Builder withWriteTimeoutMillis(long timeoutMillis) {
            Preconditions.checkArgument(timeoutMillis >= 0, "Negative value");
            this.networkTimeoutConfigBuilder.withWriteTimeout((int) timeoutMillis);

            return this;
        }

        /**
         * Sets the ExecutorService to be used by the internal tasks dispatcher.
         * <p>
         * By default, an ExecutorService is created internally.
         * In this case, it will not be customizable nor manageable by the user application.
         * It can only be shutdown by the {@link Consul#destroy()} method.
         * <p>
         * When an application needs to be able to customize the ExecutorService parameters, and/or manage its lifecycle,
         * it can provide an instance of ExecutorService to the Builder. In that case, this ExecutorService will be used instead of creating one internally.
         *
         * @param executorService The ExecutorService to be injected in the internal tasks dispatcher.
         * @return
         */
        public Builder withExecutorService(ExecutorService executorService) {
            this.executorService = executorService;

            return this;
        }


        /**
         * Sets the ConnectionPool to be used by OkHttp Client
         * <p>
         * By default, an ConnectionPool is created internally.
         * In this case, it will not be customizable nor manageable by the user application.
         * It can only be shutdown by the {@link Consul#destroy()} method.
         * <p>
         * When an application needs to be able to customize the ConnectionPool parameters, and/or manage its lifecycle,
         * it can provide an instance of ConnectionPool to the Builder. In that case, this ConnectionPool will be used instead of creating one internally.
         *
         * @param connectionPool The ConnetcionPool to be injected in the internal  OkHttpClient
         * @return
         */
        public Builder withConnectionPool(ConnectionPool connectionPool) {
            this.connectionPool = connectionPool;

            return this;
        }

        /**
         * Sets the configuration for the clients.
         * The configuration will fallback on the library default configuration if elements are not set.
         *
         * @param clientConfig the configuration to use.
         * @return The Builder
         */
        public Builder withClientConfiguration(ClientConfig clientConfig) {
            this.clientConfig = clientConfig;

            return this;
        }

        /**
         * Sets the event callback for the clients.
         * The callback will be called by the consul client after each event.
         *
         * @param callback the callback to call.
         * @return The Builder
         */
        public Builder withClientEventCallback(ClientEventCallback callback) {
            this.clientEventCallback = callback;

            return this;
        }

        /**
         * Constructs a new {@link Consul} client.
         *
         * @return A new Consul client.
         */
        public Consul build() {
            final Retrofit retrofit;

            // if an ExecutorService is provided to the Builder, we use it, otherwise, we create one
            ExecutorService executorService = this.executorService;
            if (executorService == null) {
                /**
                 * mimics okhttp3.Dispatcher#executorService implementation, except
                 * using daemon thread so shutdown is not blocked (issue #133)
                 */
                executorService = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS,
                        new SynchronousQueue<>(), Util.threadFactory("OkHttp Dispatcher", true));
            }

            if (connectionPool == null) {
                connectionPool = new ConnectionPool();
            }

            ClientConfig config = (clientConfig != null) ? clientConfig : new ClientConfig();

            OkHttpClient okHttpClient = createOkHttpClient(
                    this.sslContext,
                    this.trustManager,
                    this.hostnameVerifier,
                    this.proxy,
                    executorService,
                    connectionPool,
                    config);
            NetworkTimeoutConfig networkTimeoutConfig = new NetworkTimeoutConfig.Builder()
                    .withConnectTimeout(okHttpClient::connectTimeoutMillis)
                    .withReadTimeout(okHttpClient::readTimeoutMillis)
                    .withWriteTimeout(okHttpClient::writeTimeoutMillis)
                    .build();

            try {
                retrofit = createRetrofit(
                        buildUrl(this.url),
                        Jackson.MAPPER,
                        okHttpClient);
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }

            ClientEventCallback eventCallback = clientEventCallback != null ?
                    clientEventCallback :
                    new ClientEventCallback() {
                    };

            AgentClient agentClient = new AgentClient(retrofit, config, eventCallback);
            HealthClient healthClient = new HealthClient(retrofit, config, eventCallback, networkTimeoutConfig);
            KeyValueClient keyValueClient = new KeyValueClient(retrofit, config, eventCallback, networkTimeoutConfig);
            CatalogClient catalogClient = new CatalogClient(retrofit, config, eventCallback, networkTimeoutConfig);
            StatusClient statusClient = new StatusClient(retrofit, config, eventCallback);
            SessionClient sessionClient = new SessionClient(retrofit, config, eventCallback);
            EventClient eventClient = new EventClient(retrofit, config, eventCallback);
            PreparedQueryClient preparedQueryClient = new PreparedQueryClient(retrofit, config, eventCallback);
            CoordinateClient coordinateClient = new CoordinateClient(retrofit, config, eventCallback);
            OperatorClient operatorClient = new OperatorClient(retrofit, config, eventCallback);
            AclClient aclClient = new AclClient(retrofit, config, eventCallback);
            SnapshotClient snapshotClient = new SnapshotClient(retrofit, config, eventCallback);

            if (ping) {
                agentClient.ping();
            }
            return new Consul(agentClient, healthClient, keyValueClient,
                    catalogClient, statusClient, sessionClient, eventClient,
                    preparedQueryClient, coordinateClient, operatorClient,
                    executorService, connectionPool, aclClient, snapshotClient, okHttpClient);
        }

        private String buildUrl(URL url) {
            return url.toExternalForm().replaceAll("/$", "") + "/v1/";
        }

        private OkHttpClient createOkHttpClient(SSLContext sslContext, X509TrustManager trustManager, HostnameVerifier hostnameVerifier,
                                                Proxy proxy, ExecutorService executorService, ConnectionPool connectionPool, ClientConfig clientConfig) {

            final OkHttpClient.Builder builder = new OkHttpClient.Builder();

            if (authInterceptor != null) {
                builder.addInterceptor(authInterceptor);
            }

            if (aclTokenInterceptor != null) {
                builder.addInterceptor(aclTokenInterceptor);
            }

            if (headerInterceptor != null) {
                builder.addInterceptor(headerInterceptor);
            }

            if (consulBookendInterceptor != null) {
                builder.addInterceptor(consulBookendInterceptor);
            }

            if (consulFailoverInterceptor != null) {
                builder.addInterceptor(consulFailoverInterceptor);
            }
            builder.addInterceptor(new LoggingInterceptor(LoggingInterceptor.Level.BODY));
            if (sslContext != null && trustManager != null) {
                builder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
            } else if (sslContext != null) {
                builder.sslSocketFactory(sslContext.getSocketFactory(), TrustManagerUtils.getDefaultTrustManager());
            }

            if (hostnameVerifier != null) {
                builder.hostnameVerifier(hostnameVerifier);
            }

            if (proxy != null) {
                builder.proxy(proxy);
            }
            NetworkTimeoutConfig networkTimeoutConfig = networkTimeoutConfigBuilder.build();
            if (networkTimeoutConfig.getClientConnectTimeoutMillis() >= 0) {
                builder.connectTimeout(networkTimeoutConfig.getClientConnectTimeoutMillis(), TimeUnit.MILLISECONDS);
            }

            if (networkTimeoutConfig.getClientReadTimeoutMillis() >= 0) {
                builder.readTimeout(networkTimeoutConfig.getClientReadTimeoutMillis(), TimeUnit.MILLISECONDS);
            }

            if (networkTimeoutConfig.getClientWriteTimeoutMillis() >= 0) {
                builder.writeTimeout(networkTimeoutConfig.getClientWriteTimeoutMillis(), TimeUnit.MILLISECONDS);
            }

            builder.addInterceptor(new TimeoutInterceptor(clientConfig.getCacheConfig()));

            Dispatcher dispatcher = new Dispatcher(executorService);
            dispatcher.setMaxRequests(Integer.MAX_VALUE);
            dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);
            builder.dispatcher(dispatcher);

            if (connectionPool != null) {
                builder.connectionPool(connectionPool);
            }
            return builder.build();
        }

        private Retrofit createRetrofit(String url, ObjectMapper mapper, OkHttpClient okHttpClient) throws MalformedURLException {

            final URL consulUrl = new URL(url);

            return new Retrofit.Builder()
                    .baseUrl(new URL(consulUrl.getProtocol(), consulUrl.getHost(),
                            consulUrl.getPort(), consulUrl.getFile()).toExternalForm())
                    .addConverterFactory(JacksonConverterFactory.create(mapper))
                    .client(okHttpClient)
                    .build();
        }

    }

    private static class LoggingInterceptor implements Interceptor {

        private final Level level;
        private final static Logger logger = LoggerFactory.getLogger(LoggingInterceptor.class);

        private static final Charset UTF8 = StandardCharsets.UTF_8;


        public LoggingInterceptor() {
            this(Level.NONE);
        }

        public LoggingInterceptor(@NotNull Level level) {
            this.level = level;
        }

        @NotNull
        @Override
        public Response intercept(@NotNull Chain chain) throws IOException {
            Request request = chain.request();
            if (level == Level.NONE) {
                chain.proceed(request);
            }
            //请求日志拦截
            requestLogger(request, chain.connection());

            //执行请求，计算请求时间
            long startNs = System.nanoTime();
            Response response;
            try {
                response = chain.proceed(request);
            } catch (Exception e) {
                logger.info("<-- HTTP FAILED: " + e);
                throw e;
            }
            long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);

            //响应日志拦截
            return responseLogger(response, tookMs);
        }


        private void requestLogger(Request request, Connection connection) throws IOException {
            boolean logBody = (this.level == Level.BODY);
            boolean logHeaders = (this.level == Level.BODY || this.level == Level.HEADERS);
            RequestBody requestBody = request.body();
            boolean hasRequestBody = requestBody != null;
            Protocol protocol = connection != null ? connection.protocol() : Protocol.HTTP_1_1;

            try {
                String requestStartMessage = "--> " + request.method() + ' ' + request.url() + ' ' + protocol;
                logger.info(requestStartMessage);

                if (logHeaders) {
                    if (hasRequestBody) {
                        // Request body headers are only present when installed as a network interceptor. Force
                        // them to be included (when available) so there values are known.
                        if (requestBody.contentType() != null) {
                            logger.info("\tContent-Type: " + requestBody.contentType());
                        }
                        if (requestBody.contentLength() != -1) {
                            logger.info("\tContent-Length: " + requestBody.contentLength());
                        }
                    }
                    Headers headers = request.headers();
                    for (int i = 0, count = headers.size(); i < count; i++) {
                        String name = headers.name(i);
                        // Skip headers from the request body as they are explicitly logged above.
                        if (!"Content-Type".equalsIgnoreCase(name) && !"Content-Length".equalsIgnoreCase(name)) {
                            logger.info("\t" + name + ": " + headers.value(i));
                        }
                    }

                    logger.info(" ");
                    if (logBody && hasRequestBody) {
                        if (isPlaintext(requestBody.contentType())) {
                            bodyToString(request);
                        } else {
                            logger.error("\tbody: maybe [binary body], omitted!");
                        }
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            } finally {
                logger.info("--> END " + request.method());
            }
        }

        private void bodyToString(Request request) {
            try {
                Request copy = request.newBuilder().build();
                RequestBody body = copy.body();
                if (body == null) {
                    return;
                }
                Buffer buffer = new Buffer();
                body.writeTo(buffer);
                Charset charset = getCharset(body.contentType());
                logger.info("\tbody:" + buffer.readString(charset));
            } catch (Exception e) {
                logger.error("\tbody to string is false error: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private boolean isPlaintext(MediaType contentType) {
            if (ObjectUtils.isEmpty(contentType)) {
                return false;
            }
            if ("text".equals(contentType.type())) {
                return true;
            } else if ("application".equals(contentType.type()) && ("json".equals(contentType.subtype()) || "json".equals(contentType.subtype()) || "xml".equals(contentType.subtype())
            )) {
                return true;
            } else {
                return false;
            }

        }

        private Response responseLogger(Response response, long tookMs) {
            Response.Builder builder = response.newBuilder();
            Response clone = builder.build();
            ResponseBody responseBody = clone.body();
            boolean hasRequestBody = responseBody != null;
            boolean logBody = (this.level == Level.BODY);
            boolean logHeaders = (this.level == Level.BODY || this.level == Level.HEADERS);

            try {
                logger.info("<-- " + clone.code() + ' ' + clone.message() + ' ' + clone.request().url() + " (" + tookMs + "ms）");
                if (logHeaders) {
                    Headers headers = clone.headers();
                    for (int i = 0, count = headers.size(); i < count; i++) {
                        logger.info("\t" + headers.name(i) + ": " + headers.value(i));
                    }
                    logger.info(" ");
                    if (logBody && HttpHeaders.promisesBody(clone)) {
                        if (responseBody == null) {
                            return response;
                        }

                        if (isPlaintext(responseBody.contentType())) {
                            byte[] bytes = responseBody.bytes();
                            MediaType contentType = responseBody.contentType();
                            String body = new String(bytes, getCharset(contentType));
                            logger.info("\tbody:" + body);
                            responseBody = ResponseBody.create(bytes, responseBody.contentType());
                            return response.newBuilder().body(responseBody).build();
                        } else {
                            logger.info("\tbody: maybe [binary body], omitted!");
                        }
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            } finally {
                logger.info("<-- END HTTP");
            }
            return response;
        }

        private static Charset getCharset(MediaType contentType) {
            Charset charset = contentType != null ? contentType.charset(UTF8) : UTF8;
            if (charset == null) {
                charset = UTF8;
            }
            return charset;
        }

        public enum Level {
            NONE,       //不打印log
            BASIC,      //只打印 请求首行 和 响应首行
            HEADERS,    //打印请求和响应的所有 Header
            BODY        //所有数据全部打印
        }
    }

    public static class NetworkTimeoutConfig {
        private final IntSupplier readTimeoutMillisSupplier;
        private final IntSupplier writeTimeoutMillisSupplier;
        private final IntSupplier connectTimeoutMillisSupplier;

        private NetworkTimeoutConfig(
                IntSupplier readTimeoutMillisSupplier,
                IntSupplier writeTimeoutMillisSupplier,
                IntSupplier connectTimeoutMillisSupplier) {
            this.readTimeoutMillisSupplier = readTimeoutMillisSupplier;
            this.writeTimeoutMillisSupplier = writeTimeoutMillisSupplier;
            this.connectTimeoutMillisSupplier = connectTimeoutMillisSupplier;
        }

        public int getClientReadTimeoutMillis() {
            return readTimeoutMillisSupplier.getAsInt();
        }

        public int getClientWriteTimeoutMillis() {
            return writeTimeoutMillisSupplier.getAsInt();
        }

        public int getClientConnectTimeoutMillis() {
            return connectTimeoutMillisSupplier.getAsInt();
        }

        public static class Builder {
            private IntSupplier readTimeoutMillisSupplier = () -> -1;
            private IntSupplier writeTimeoutMillisSupplier = () -> -1;
            private IntSupplier connectTimeoutMillisSupplier = () -> -1;

            public NetworkTimeoutConfig.Builder withReadTimeout(IntSupplier timeoutSupplier) {
                this.readTimeoutMillisSupplier = timeoutSupplier;
                return this;
            }

            public NetworkTimeoutConfig.Builder withReadTimeout(int millis) {
                return withReadTimeout(() -> millis);
            }

            public NetworkTimeoutConfig.Builder withWriteTimeout(IntSupplier timeoutSupplier) {
                this.writeTimeoutMillisSupplier = timeoutSupplier;
                return this;
            }

            public NetworkTimeoutConfig.Builder withWriteTimeout(int millis) {
                return withWriteTimeout(() -> millis);
            }

            public NetworkTimeoutConfig.Builder withConnectTimeout(IntSupplier timeoutSupplier) {
                this.connectTimeoutMillisSupplier = timeoutSupplier;
                return this;
            }

            public NetworkTimeoutConfig.Builder withConnectTimeout(int millis) {
                return withConnectTimeout(() -> millis);
            }

            public NetworkTimeoutConfig build() {
                return new NetworkTimeoutConfig(readTimeoutMillisSupplier, writeTimeoutMillisSupplier, connectTimeoutMillisSupplier);
            }
        }
    }
}
