package top.lytree.consul;

import com.google.common.collect.ImmutableMap;
import top.lytree.consul.async.ConsulResponseCallback;
import top.lytree.consul.config.ClientConfig;
import top.lytree.consul.model.ConsulResponse;
import top.lytree.consul.model.State;
import top.lytree.consul.model.health.HealthCheck;
import top.lytree.consul.model.health.ServiceHealth;
import top.lytree.consul.monitoring.ClientEventCallback;
import top.lytree.consul.option.QueryOptions;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.http.GET;
import retrofit2.http.HeaderMap;
import retrofit2.http.Path;
import retrofit2.http.Query;
import retrofit2.http.QueryMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP Client for /v1/health/ endpoints.
 * 启用健康检查时查询节点运行状况。
 */
public class HealthClient extends BaseCacheableClient {

    private static final String CLIENT_NAME = "health";

    private final Api api;

    /**
     * Constructs an instance of this class.
     *
     * @param retrofit The {@link Retrofit} to build a client from.
     */
    HealthClient(Retrofit retrofit, ClientConfig config, ClientEventCallback eventCallback, Consul.NetworkTimeoutConfig networkTimeoutConfig) {
        super(CLIENT_NAME, config, eventCallback, networkTimeoutConfig);
        this.api = retrofit.create(Api.class);
    }

/**
     * 检索节点的运行状况检查。
     * <p/>
     * 获取 /v1/health/node/{node}
     *
     * @param节点 返回检查的节点
     * @return 包含以下列表的 {@link top.lytree.consul.model.ConsulResponse}
     * {@link top.lytree.consul.model.health.HealthCheck} 对象。
     */
    public ConsulResponse<List<HealthCheck>> getNodeChecks(String node) {
        return getNodeChecks(node, QueryOptions.BLANK);
    }

/**
     * 使用 {@link top.lytree.consul.option.QueryOptions} 检索给定数据中心中节点的运行状况检查。
     * <p/>
     * GET /v1/health/node/{node}？dc={数据中心}
     *
     * @param node 返回检查的节点
     * @param queryOptions 要使用的查询选项。
     * @return 包含以下列表的 {@link top.lytree.consul.model.ConsulResponse}
     * {@link top.lytree.consul.model.health.HealthCheck} 对象。
     */
    public ConsulResponse<List<HealthCheck>> getNodeChecks(String node,
                                                           QueryOptions queryOptions) {
        return http.extractConsulResponse(api.getNodeChecks(node, queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()));
    }

/**
     * 检索服务的运行状况检查。
     * <p/>
     * 获取 /v1/health/checks/{service}
     *
     * @return 包含以下列表的 {@link top.lytree.consul.model.ConsulResponse}
     * {@link top.lytree.consul.model.health.HealthCheck} 对象。
     */
    public ConsulResponse<List<HealthCheck>> getServiceChecks(String service) {
        return getServiceChecks(service, QueryOptions.BLANK);
    }

/**
     * 使用 {@link top.lytree.consul.option.QueryOptions} 检索给定数据中心中服务的运行状况检查。
     * <p/>
     * GET /v1/health/checks/{service}？dc={数据中心}
     *
     * @param queryOptions 要使用的查询选项。
     * @return 包含以下列表的 {@link top.lytree.consul.model.ConsulResponse}
     * {@link top.lytree.consul.model.health.HealthCheck} 对象。
     */
    public ConsulResponse<List<HealthCheck>> getServiceChecks(String service,
                                                              QueryOptions queryOptions) {
        return http.extractConsulResponse(api.getServiceChecks(service, queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()));
    }

    /**
     * Asynchronously retrieves the healthchecks for a service in a given
     * datacenter with {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/health/checks/{service}?dc={datacenter}
     *
     * @param service      The service to query.
     * @param queryOptions The Query Options to use.
     * @param callback     Callback implemented by callee to handle results.
     *                     {@link top.lytree.consul.model.health.HealthCheck} objects.
     */
    public void getServiceChecks(String service,
                                 QueryOptions queryOptions,
                                 ConsulResponseCallback<List<HealthCheck>> callback) {
        http.extractConsulResponse(api.getServiceChecks(service, queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()), callback);
    }

    /**
     * Retrieves the healthchecks for a state.
     * <p/>
     * GET /v1/health/state/{state}
     *
     * @param state The state to query.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a list of
     * {@link top.lytree.consul.model.health.HealthCheck} objects.
     */
    public ConsulResponse<List<HealthCheck>> getChecksByState(State state) {
        return getChecksByState(state, QueryOptions.BLANK);
    }

    /**
     * Retrieves the healthchecks for a state in a given datacenter with {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/health/state/{state}?dc={datacenter}
     *
     * @param state        The state to query.
     * @param queryOptions The Query Options to use.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a list of
     * {@link top.lytree.consul.model.health.HealthCheck} objects.
     */
    public ConsulResponse<List<HealthCheck>> getChecksByState(State state,
                                                              QueryOptions queryOptions) {
        return http.extractConsulResponse(api.getChecksByState(state.getName(), queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()));
    }

    /**
     * Asynchronously retrieves the healthchecks for a state in a given datacenter with {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/health/state/{state}?dc={datacenter}
     *
     * @param state        The state to query.
     * @param queryOptions The Query Options to use.
     * @param callback     Callback implemented by callee to handle results.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a list of
     * {@link top.lytree.consul.model.health.HealthCheck} objects.
     */
    public void getChecksByState(State state, QueryOptions queryOptions,
                                 ConsulResponseCallback<List<HealthCheck>> callback) {
        http.extractConsulResponse(api.getChecksByState(state.getName(), queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()), callback);
    }

    /**
     * Retrieves the healthchecks for all healthy service instances.
     * <p/>
     * GET /v1/health/service/{service}?passing
     *
     * @param service The service to query.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a list of
     * {@link top.lytree.consul.model.health.HealthCheck} objects.
     */
    public ConsulResponse<List<ServiceHealth>> getHealthyServiceInstances(String service) {
        return getHealthyServiceInstances(service, QueryOptions.BLANK);
    }

    /**
     * Retrieves the healthchecks for all healthy service instances in a given datacenter with
     * {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/health/service/{service}?dc={datacenter}&amp;passing
     *
     * @param service      The service to query.
     * @param queryOptions The Query Options to use.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a list of
     * {@link top.lytree.consul.model.health.HealthCheck} objects.
     */
    public ConsulResponse<List<ServiceHealth>> getHealthyServiceInstances(String service,
                                                                          QueryOptions queryOptions) {
        return http.extractConsulResponse(api.getServiceInstances(service,
                optionsFrom(ImmutableMap.of("passing", "true"), queryOptions.toQuery()),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()));
    }


    /**
     * Asynchronously retrieves the healthchecks for all healthy service instances in a given
     * datacenter with {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/health/service/{service}?dc={datacenter}&amp;passing
     * <p/>
     * Experimental.
     *
     * @param service      The service to query.
     * @param queryOptions The Query Options to use.
     * @param callback     Callback implemented by callee to handle results.
     */
    public void getHealthyServiceInstances(String service, QueryOptions queryOptions,
                                           ConsulResponseCallback<List<ServiceHealth>> callback) {
        http.extractConsulResponse(api.getServiceInstances(service,
                optionsFrom(ImmutableMap.of("passing", "true"), queryOptions.toQuery()),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()), callback);
    }

    /**
     * Retrieves the healthchecks for all nodes.
     * <p/>
     * GET /v1/health/service/{service}
     *
     * @param service The service to query.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a list of
     * {@link top.lytree.consul.model.health.HealthCheck} objects.
     */
    public ConsulResponse<List<ServiceHealth>> getAllServiceInstances(String service) {
        return getAllServiceInstances(service, QueryOptions.BLANK);
    }

    /**
     * Retrieves the healthchecks for all nodes in a given datacenter with
     * {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/health/service/{service}?dc={datacenter}
     *
     * @param service      The service to query.
     * @param queryOptions The Query Options to use.
     * @return A {@link top.lytree.consul.model.ConsulResponse} containing a list of
     * {@link top.lytree.consul.model.health.HealthCheck} objects.
     */
    public ConsulResponse<List<ServiceHealth>> getAllServiceInstances(String service, QueryOptions queryOptions) {
        return http.extractConsulResponse(api.getServiceInstances(service, queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()));
    }

    /**
     * Asynchronously retrieves the healthchecks for all nodes in a given
     * datacenter with {@link top.lytree.consul.option.QueryOptions}.
     * <p/>
     * GET /v1/health/service/{service}?dc={datacenter}
     * <p/>
     * Experimental.
     *
     * @param service      The service to query.
     * @param queryOptions The Query Options to use.
     * @param callback     Callback implemented by callee to handle results.
     */
    public void getAllServiceInstances(String service, QueryOptions queryOptions,
                                       ConsulResponseCallback<List<ServiceHealth>> callback) {
        http.extractConsulResponse(api.getServiceInstances(service, queryOptions.toQuery(),
                queryOptions.getTag(), queryOptions.getNodeMeta(), queryOptions.toHeaders()), callback);
    }

    private static Map<String, Object> optionsFrom(Map<String, ?>... options) {
        Map<String, Object> result = new HashMap<>();

        for (Map<String, ?> option : options) {
            result.putAll(option);
        }

        return result;
    }

    /**
     * Retrofit API interface.
     */
    interface Api {

        @GET("health/node/{node}")
        Call<List<HealthCheck>> getNodeChecks(@Path("node") String node,
                                              @QueryMap Map<String, Object> query,
                                              @Query("tag") List<String> tag,
                                              @Query("node-meta") List<String> nodeMeta,
                                              @HeaderMap Map<String, String> headers);

        @GET("health/checks/{service}")
        Call<List<HealthCheck>> getServiceChecks(@Path("service") String service,
                                                 @QueryMap Map<String, Object> query,
                                                 @Query("tag") List<String> tag,
                                                 @Query("node-meta") List<String> nodeMeta,
                                                 @HeaderMap Map<String, String> headers);

        @GET("health/state/{state}")
        Call<List<HealthCheck>> getChecksByState(@Path("state") String state,
                                                 @QueryMap Map<String, Object> query,
                                                 @Query("tag") List<String> tag,
                                                 @Query("node-meta") List<String> nodeMeta,
                                                 @HeaderMap Map<String, String> headers);

        @GET("health/service/{service}")
        Call<List<ServiceHealth>> getServiceInstances(@Path("service") String service,
                                                      @QueryMap Map<String, Object> query,
                                                      @Query("tag") List<String> tag,
                                                      @Query("node-meta") List<String> nodeMeta,
                                                      @HeaderMap Map<String, String> headers);
    }
}
