package top.lytree.consul.model.session;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import top.lytree.consul.util.SecondsDeserializer;
import top.lytree.consul.util.SecondsSerializer;
import org.immutables.value.Value;

import java.util.List;

@Value.Immutable
@JsonSerialize(as = ImmutableSessionInfo.class)
@JsonDeserialize(as = ImmutableSessionInfo.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class SessionInfo {

    @JsonProperty("CreateIndex")
    public abstract long getCreateIndex();

    @JsonProperty("LockDelay")
    @JsonSerialize(using = SecondsSerializer.class)
    @JsonDeserialize(using = SecondsDeserializer.class)
    public abstract Long getLockDelay();

    @JsonProperty("Name")
    public abstract Optional<String> getName();

    @JsonProperty("Node")
    public abstract String getNode();

    @JsonProperty("Checks")
    public abstract List<String> getChecks();

    @JsonProperty("Behavior")
    public abstract String getBehavior();

    @JsonProperty("TTL")
    public abstract Optional<String> getTtl();

    @JsonProperty("ID")
    public abstract String getId();

}
