package top.lytree.consul;

import com.google.common.collect.ImmutableMap;
import top.lytree.consul.config.ClientConfig;
import top.lytree.consul.model.operator.RaftConfiguration;
import top.lytree.consul.monitoring.ClientEventCallback;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.http.DELETE;
import retrofit2.http.GET;
import retrofit2.http.Query;
import retrofit2.http.QueryMap;

import java.util.Map;

/**
 * 执行集群级别的任务，例如与 Raft 子系统交互或获取许可证信息。
 */
public class OperatorClient extends BaseClient {

    private static final String CLIENT_NAME = "operator";

    private final Api api;

    OperatorClient(Retrofit retrofit, ClientConfig config, ClientEventCallback eventCallback) {
        super(CLIENT_NAME, config, eventCallback);
        this.api = retrofit.create(Api.class);
    }

    public RaftConfiguration getRaftConfiguration() {
        return http.extract(api.getConfiguration(ImmutableMap.of()));
    }

    public RaftConfiguration getRaftConfiguration(String datacenter) {
        return http.extract(api.getConfiguration(ImmutableMap.of("dc", datacenter)));
    }

    public RaftConfiguration getStaleRaftConfiguration(String datacenter) {
        return http.extract(api.getConfiguration(ImmutableMap.of(
            "dc", datacenter, "stale", "true"
        )));
    }

    public RaftConfiguration getStaleRaftConfiguration() {
        return http.extract(api.getConfiguration(ImmutableMap.of(
                "stale", "true"
        )));
    }

    public void deletePeer(String address) {
        http.handle(api.deletePeer(address, ImmutableMap.of()));
    }

    public void deletePeer(String address, String datacenter) {
        http.handle(api.deletePeer(address, ImmutableMap.of("dc", datacenter)));
    }

    interface Api {

        @GET("operator/raft/configuration")
        Call<RaftConfiguration> getConfiguration(@QueryMap Map<String, String> query);

        @DELETE("operator/raft/peer")
        Call<Void> deletePeer(@Query("address") String address,
                              @QueryMap Map<String, String> query);
    }
}
