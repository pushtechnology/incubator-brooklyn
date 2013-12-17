package brooklyn.entity.nosql.mongodb;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.config.render.RendererHints;
import brooklyn.entity.basic.SoftwareProcessImpl;
import brooklyn.event.SensorEvent;
import brooklyn.event.SensorEventListener;
import brooklyn.event.feed.function.FunctionFeed;
import brooklyn.event.feed.function.FunctionPollConfig;
import brooklyn.util.collections.MutableMap;

import com.google.common.base.Functions;
import com.google.common.base.Objects;

public class MongoDBServerImpl extends SoftwareProcessImpl implements MongoDBServer {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBServerImpl.class);

    static {
        RendererHints.register(HTTP_INTERFACE_URL, new RendererHints.NamedActionWithUrl("Open"));
    }

    private FunctionFeed serviceStats;
    private FunctionFeed replicaSetStats;
    private MongoDBClientSupport client;

    public MongoDBServerImpl() {
    }

    @Override
    public Class<?> getDriverInterface() {
        return MongoDBDriver.class;
    }

    @Override
    protected void connectSensors() {
        super.connectSensors();
        connectServiceUpIsRunning();

        int httpConsolePort = getAttribute(PORT) + 1000;
        if (httpConsolePort != getAttribute(HTTP_PORT)+1000) {
            // see comment on HTTP_PORT
            LOG.warn(this+" may not have opened HTTP_PORT correctly as the default was not available");
            setAttribute(HTTP_PORT, httpConsolePort);
        }
        setAttribute(HTTP_INTERFACE_URL, String.format("http://%s:%d", getAttribute(HOSTNAME), httpConsolePort));
        setAttribute(MONGO_SERVER_ENDPOINT, String.format("http://%s:%d", getAttribute(HOSTNAME), getAttribute(MongoDBServer.PORT)));

        try {
            client = MongoDBClientSupport.forServer(this);
        } catch (UnknownHostException e) {
            LOG.warn("Unable to create client connection to {}, not connecting sensors: {} ", this, e.getMessage());
            return;
        }

        serviceStats = FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<Object, BasicBSONObject>(STATUS_BSON)
                        .period(2, TimeUnit.SECONDS)
                        .callable(new Callable<BasicBSONObject>() {
                            @Override
                            public BasicBSONObject call() throws Exception {
                                return MongoDBServerImpl.this.getAttribute(SERVICE_UP)
                                    ? client.getServerStatus()
                                    : null;
                            }
                        })
                        .onException(Functions.<BasicBSONObject>constant(null)))
                .build();

        final boolean replicaSetEnabled = getConfig(REPLICA_SET_ENABLED);
        if (replicaSetEnabled) {
            replicaSetStats = FunctionFeed.builder()
                    .entity(this)
                    .poll(new FunctionPollConfig<Object, ReplicaSetMemberStatus>(REPLICA_SET_MEMBER_STATUS)
                            .period(2, TimeUnit.SECONDS)
                            .callable(new Callable<ReplicaSetMemberStatus>() {
                                /**
                                 * Calls {@link MongoDBClientSupport#getReplicaSetStatus} and
                                 * extracts <code>myState</code> from the response.
                                 * @return
                                 *      The appropriate {@link brooklyn.entity.nosql.mongodb.ReplicaSetMemberStatus}
                                 *      if <code>myState</code> was non-null, {@link ReplicaSetMemberStatus#UNKNOWN} otherwise.
                                 */
                                @Override
                                public ReplicaSetMemberStatus call() {
                                    BasicBSONObject serverStatus = client.getReplicaSetStatus();
                                    int state = serverStatus.getInt("myState", -1);
                                    return ReplicaSetMemberStatus.fromCode(state);
                                }
                            })
                            .onException(Functions.constant(ReplicaSetMemberStatus.UNKNOWN)))
                    .build();
        } else {
            setAttribute(IS_PRIMARY_FOR_REPLICA_SET, false);
            setAttribute(IS_SECONDARY_FOR_REPLICA_SET, false);
        }

        // Take interesting details from STATUS.
        subscribe(this, STATUS_BSON, new SensorEventListener<BasicBSONObject>() {
                @Override public void onEvent(SensorEvent<BasicBSONObject> event) {
                    BasicBSONObject map = event.getValue();
                    setAttribute(STATUS_JSON, MutableMap.copyOf(map));
                    if (map != null && !map.isEmpty()) {
                        setAttribute(UPTIME_SECONDS, map.getDouble("uptime", 0));

                        // Operations
                        BasicBSONObject opcounters = (BasicBSONObject) map.get("opcounters");
                        setAttribute(OPCOUNTERS_INSERTS, opcounters.getLong("insert", 0));
                        setAttribute(OPCOUNTERS_QUERIES, opcounters.getLong("query", 0));
                        setAttribute(OPCOUNTERS_UPDATES, opcounters.getLong("update", 0));
                        setAttribute(OPCOUNTERS_DELETES, opcounters.getLong("delete", 0));
                        setAttribute(OPCOUNTERS_GETMORE, opcounters.getLong("getmore", 0));
                        setAttribute(OPCOUNTERS_COMMAND, opcounters.getLong("command", 0));

                        // Network stats
                        BasicBSONObject network = (BasicBSONObject) map.get("network");
                        setAttribute(NETWORK_BYTES_IN, network.getLong("bytesIn", 0));
                        setAttribute(NETWORK_BYTES_OUT, network.getLong("bytesOut", 0));
                        setAttribute(NETWORK_NUM_REQUESTS, network.getLong("numRequests", 0));

                        // Replica set stats
                        BasicBSONObject repl = (BasicBSONObject) map.get("repl");
                        if (replicaSetEnabled && repl != null) {
                            setAttribute(IS_PRIMARY_FOR_REPLICA_SET, repl.getBoolean("ismaster"));
                            setAttribute(IS_SECONDARY_FOR_REPLICA_SET, repl.getBoolean("secondary"));
                            setAttribute(REPLICA_SET_PRIMARY_ENDPOINT, repl.getString("primary"));
                        }
                    }
                }
        });
    }

    @Override
    protected void disconnectSensors() {
        disconnectServiceUpIsRunning();
        if (serviceStats != null) serviceStats.stop();
        if (replicaSetStats != null) replicaSetStats.stop();
        try {
            if (client != null) client.close();
        } catch (IOException e) {
            LOG.debug("Exception closing server connection: " + e.getMessage());
        }
    }

    @Override
    public MongoDBClientSupport getClient() {
        return client;
    }

    @Override
    public MongoDBReplicaSet getReplicaSet() {
        return Boolean.TRUE.equals(getConfig(MongoDBServer.REPLICA_SET_ENABLED))
                ? getConfig(MongoDBServer.REPLICA_SET)
                : null;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", getId())
                .add("hostname", getAttribute(HOSTNAME))
                .add("port", getAttribute(PORT))
                .toString();
    }

}
