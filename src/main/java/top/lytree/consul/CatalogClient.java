package top.lytree.consul;

import top.lytree.consul.async.ConsulResponseCallback;
import top.lytree.consul.config.ClientConfig;
import top.lytree.consul.model.ConsulResponse;
import top.lytree.consul.model.catalog.CatalogDeregistration;
import top.lytree.consul.model.catalog.CatalogNode;
import top.lytree.consul.model.catalog.CatalogRegistration;
import top.lytree.consul.model.catalog.CatalogService;
import top.lytree.consul.model.health.Node;
import top.lytree.consul.monitoring.ClientEventCallback;
import top.lytree.consul.option.QueryOptions;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.http.*;

import java.util.List;
import java.util.Map;

/**
 * HTTP Client for /v1/catalog/ endpoints.
 * 注册和注销节点、服务和运行状况检查。
 */
public class CatalogClient extends BaseCacheableClient {

    private static final String CLIENT_NAME = "catalog";

    private final Api api;

    /**
     * Constructs an instance of this class.
     *
     * @param retrofit The {@link Retrofit} to build a client from.
     */
    CatalogClient(Retrofit retrofit, ClientConfig config, ClientEventCallback eventCallback, Consul.NetworkTimeoutConfig networkTimeoutConfig) {
        super(CLIENT_NAME, config, eventCallback, networkTimeoutConfig);
        this.api = retrofit.create(Api.class);
    }

    /**
     * Retrieves all datacenters.
     * <p/>
     * GET /v1/catalog/datacenters
     *
     * @return A list of datacenter names.
     */
    public List<String> getDatacenters() {
        return getDatacenters(QueryOptions.BLANK);
    }

    /**
     * Get the list of datacenters with query options
     * @param queryOptions
     * @return
     */
    public List<String> getDatacenters(QueryOptions queryOptions) {
        return http.extract(api.getDatacenters(queryOptions.toHeaders()));
    }

    /**
     * Retrieves all nodes.
     * <p/>
     * GET /v1/catalog/nodes
     *
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a list of
     * {@link top.lytree.consul.model.health.Node} objects.
     */
    public ConsulResponse<List<Node>> getNodes() {
        return getNodes(QueryOptions.BLANK);
    }

    /**
     * Retrieves all nodes for a given datacenter with {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/catalog/nodes?dc={datacenter}
     *
     * @param queryOptions The Query Options to use.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a list of
     * {@link top.lytree.consul.model.health.Node} objects.
     */
    public ConsulResponse<List<Node>> getNodes(QueryOptions queryOptions) {
        return http.extractConsulResponse(api.getNodes(queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(),
                queryOptions.toHeaders()));
    }

    /**
     * Asynchronously retrieves the nodes for a given datacenter with {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/catalog/nodes?dc={datacenter}
     *
     * @param queryOptions The Query Options to use.
     * @param callback     Callback implemented by callee to handle results.
     *                     {@link top.lytree.consul.model.health.Node} objects.
     */
    public void getNodes(QueryOptions queryOptions, ConsulResponseCallback<List<Node>> callback) {
        http.extractConsulResponse(api.getNodes(queryOptions.toQuery(), queryOptions.getTag(),
                queryOptions.getNodeMeta(), queryOptions.toHeaders()), callback);
    }

    /**
     * Retrieves all services for a given datacenter.
     * <p/>
     * GET /v1/catalog/services?dc={datacenter}
     *
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a map of service name to list of tags.
     */
    public ConsulResponse<Map<String, List<String>>> getServices() {
        return getServices(QueryOptions.BLANK);
    }

    /**
     * Asynchronously retrieves the services for a given datacenter.
     * <p/>
     * GET /v1/catalog/services?dc={datacenter}
     *
     * @param callback     Callback implemented by callee to handle results.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a map of service name to list of tags.
     */
    public void getServices(ConsulResponseCallback<Map<String, List<String>>> callback) {
        getServices(QueryOptions.BLANK, callback);
    }

    /**
     * Retrieves all services for a given datacenter.
     * <p/>
     * GET /v1/catalog/services?dc={datacenter}
     *
     * @param queryOptions The Query Options to use.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a map of service name to list of tags.
     */
    public ConsulResponse<Map<String, List<String>>> getServices(QueryOptions queryOptions) {
        return http.extractConsulResponse(api.getServices(queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()));
    }

    /**
     * Asynchronously retrieves the services for a given datacenter.
     * <p/>
     * GET /v1/catalog/services?dc={datacenter}
     *
     * @param queryOptions The Query Options to use.
     * @param callback     Callback implemented by callee to handle results.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a map of service name to list of tags.
     */
    public void getServices(QueryOptions queryOptions, ConsulResponseCallback<Map<String, List<String>>> callback) {
        http.extractConsulResponse(api.getServices(queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()), callback);
    }

    /**
     * Retrieves the single service.
     * <p/>
     * GET /v1/catalog/service/{service}
     *
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing
     * {@link top.lytree.consul.model.catalog.CatalogService} objects.
     */
    public ConsulResponse<List<CatalogService>> getService(String service) {
        return getService(service, QueryOptions.BLANK);
    }

    /**
     * Retrieves a single service for a given datacenter with {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/catalog/service/{service}?dc={datacenter}
     *
     * @param queryOptions The Query Options to use.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing
     * {@link top.lytree.consul.model.catalog.CatalogService} objects.
     */
    public ConsulResponse<List<CatalogService>> getService(String service, QueryOptions queryOptions) {
        return http.extractConsulResponse(api.getService(service, queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()));
    }

    /**
     * Asynchronously retrieves the single service for a given datacenter with {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/catalog/service/{service}?dc={datacenter}
     *
     * @param queryOptions The Query Options to use.
     * @param callback     Callback implemented by callee to handle results.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing
     * {@link top.lytree.consul.model.catalog.CatalogService} objects.
     */
    public void getService(String service, QueryOptions queryOptions, ConsulResponseCallback<List<CatalogService>> callback) {
        http.extractConsulResponse(api.getService(service, queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()), callback);
    }

    /**
     * Retrieves a single node.
     * <p/>
     * GET /v1/catalog/node/{node}
     *
     * @return A list of matching {@link top.lytree.consul.model.catalog.CatalogService} objects.
     */
    public ConsulResponse<CatalogNode> getNode(String node) {
        return getNode(node, QueryOptions.BLANK);
    }

    /**
     * Retrieves a single node for a given datacenter with {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/catalog/node/{node}?dc={datacenter}
     *
     * @param queryOptions The Query Options to use.
     * @return A list of matching {@link top.lytree.consul.model.catalog.CatalogService} objects.
     */
    public ConsulResponse<CatalogNode> getNode(String node, QueryOptions queryOptions) {
        return http.extractConsulResponse(api.getNode(node, queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()));
    }

    /**
     * Asynchronously retrieves the single node for a given datacenter with {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/catalog/node/{node}?dc={datacenter}
     *
     * @param queryOptions The Query Options to use.
     * @param callback     Callback implemented by callee to handle results.
     */
    public void getNode(String node, QueryOptions queryOptions, ConsulResponseCallback<CatalogNode> callback) {
        http.extractConsulResponse(api.getNode(node, queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()), callback);
    }

    /**
     * Registers a service or node.
     * <p/>
     * PUT /v1/catalog/register
     *
     * @param registration A {@link CatalogRegistration}
     */
    public void register(CatalogRegistration registration) {
        register(registration, QueryOptions.BLANK);
    }

    /**
     * Registers a service or node.
     * <p/>
     * PUT /v1/catalog/register
     *
     * @param registration A {@link CatalogRegistration}
     */
    public void register(CatalogRegistration registration, QueryOptions options) {
        http.handle(api.register(registration, options.toQuery()));
    }

    /**
     * Deregisters a service or node.
     * <p/>
     * PUT /v1/catalog/deregister
     *
     * @param deregistration A {@link CatalogDeregistration}
     */
    public void deregister(CatalogDeregistration deregistration) {
        deregister(deregistration, QueryOptions.BLANK);
    }

    /**
     * Deregisters a service or node.
     * <p/>
     * PUT /v1/catalog/deregister
     *
     * @param deregistration A {@link CatalogDeregistration}
     */
    public void deregister(CatalogDeregistration deregistration, QueryOptions options) {
        http.handle(api.deregister(deregistration, options.toQuery()));
    }

    /**
     * Retrofit API interface.
     */
    interface Api {

        @GET("catalog/datacenters")
        Call<List<String>> getDatacenters(@HeaderMap Map<String, String> headers);

        @GET("catalog/nodes")
        Call<List<Node>> getNodes(@QueryMap Map<String, Object> query,
                                  @Query("tag") List<String> tag,
                                  @Query("node-meta") List<String> nodeMeta,
                                  @HeaderMap Map<String, String> headers);

        @GET("catalog/node/{node}")
        Call<CatalogNode> getNode(@Path("node") String node,
                                  @QueryMap Map<String, Object> query,
                                  @Query("tag") List<String> tag,
                                  @Query("node-meta") List<String> nodeMeta,
                                  @HeaderMap Map<String, String> headers);

        @GET("catalog/services")
        Call<Map<String, List<String>>> getServices(@QueryMap Map<String, Object> query,
                                                    @Query("tag") List<String> tag,
                                                    @Query("node-meta") List<String> nodeMeta,
                                                    @HeaderMap Map<String, String> headers);

        @GET("catalog/service/{service}")
        Call<List<CatalogService>> getService(@Path("service") String service,
                                              @QueryMap Map<String, Object> queryMeta,
                                              @Query("tag") List<String> tag,
                                              @Query("node-meta") List<String> nodeMeta,
                                              @HeaderMap Map<String, String> headers);

        @PUT("catalog/register")
        Call<Void> register(@Body CatalogRegistration registration, @QueryMap Map<String, Object> options);

        @PUT("catalog/deregister")
        Call<Void> deregister(@Body CatalogDeregistration deregistration, @QueryMap Map<String, Object> options);


    }
}