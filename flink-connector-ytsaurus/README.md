# Apache Flink YTsaurus Connector

This project contains the Apache Flink Connector for working with [YTsaurus Dynamic Tables](https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/overview).

## Flink APIs

- Stream API - only data writing is supported;
- Table API/SQL - bounded stream reading, writing, and lookup are supported.

## Features
 
- writing to YTsaurus dynamic tables
- automatic table creation before writing
- advanced table sharding
- data partitioning with various granularity
- external lock service support
- sync/async lookup in YTsaurus dynamic tables
- lookups with cache in ```FULL``` and ```PARTIAL``` modes

## Supported Flink Versions

The connector officially supports Apache Flink version `1.20.1`, but compatibility with other, earlier versions is not excluded.

## Building from Source

Prerequisites:

* Git
* Gradle 8.x (we recommend 8.14.3)
* Java 11

```
git clone https://github.com/ytsaurus/ytsaurus-flyt.git
cd ytsaurus-flyt
./gradlew shadowJar -p flink-connector-ytsaurus

# To see the assembled artifact
ls flink-connector-ytsaurus/build/libs
```

## How to try

#### Step 1 - Installing YTsaurus

At this stage, we need to install a YTsaurus cluster to which we will connect. This step can be skipped if you already have a cluster configured.

We recommend using the official documentation to [install the YTsaurus cluster via Kind](https://ytsaurus.tech/docs/en/overview/try-yt?tabs=defaultTabsGroup-5fe2y4hn_kind_dropdown).

> [!NOTE]
> The flink-connector-ytsaurus uses a [Java YTsaurus client](https://ytsaurus.tech/docs/en/api/java/examples) that uses RPC proxy.

To open access to RPC proxy, we need to modify [the configuration from the instructions](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/cluster_v1_local.yaml) (Step 5). You need to add service `NodePort` to the `rpcProxies` section.

```yaml
  rpcProxies:
    - instanceCount: 1
      serviceType: NodePort
      loggers: *loggers
      role: default
    - instanceCount: 1
      loggers: *loggers
      role: heavy
```

#### Step 2 - Setting up the YTsaurus cluster

After starting the YTsaurus cluster, we need to configure access to RPC proxy.
We need to set the balancer address of the RPC proxy to which YTsaurus will forward our requests.

```commandline
yt --proxy localhost:8082 set --format json //sys/rpc_proxies/@balancers '{ "default": { "internal_rpc": { "default": ["localhost:8013"]} } }'
```

Now we can forward the port to our RPC proxy.

```commandline
kubectl port-forward service/rpc-proxies-lb 8013:9013
```

We will place the HTTP proxy on port 8082 since port 8081 will be occupied by Apache Flink.

```commandline
kubectl port-forward service/http-proxies-lb 8082:80
```

#### Step 3 - Installing Apache Flink cluster

At this stage, we need to install Apache Flink cluster.

Please use the [official documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/try-flink/local_installation/#downloading-flink) and check [Supported Flink Version](#supported-flink-versions).

#### Step 4 - Install Flink Connector YTsaurus to Apache Flink cluster

Check out section [Building from Source](#building-from-source) to learn how to build the connector from source code.
After assembling the connector, you need to place the resulting jar file in the directory `{$FLINK_SOURCE}/lib`.

#### Step 5 - Start Apache Flink Cluster with Flink SQL Client

Start Apache Flink cluster with command `./bin/start-cluster.sh`.
Start Flink SQL Client `./bin/sql-client.sh`.

#### Step 6 - Run Demo Job

1) Create Datagen source

```SQL
CREATE TABLE simple_datagen_source (
    id BIGINT,
    name STRING,
    age INT,
    salary DOUBLE,
    is_active BOOLEAN,
    created_at TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '50',
    'number-of-rows' = '1000',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '100000',
    'fields.name.length' = '20',
    'fields.age.min' = '22',
    'fields.age.max' = '65',
    'fields.salary.min' = '30000.0',
    'fields.salary.max' = '150000.0'
);
```

2) Create YTsaurus sink

```SQL
CREATE TABLE ytsaurus_simple_sink (
    id BIGINT,
    name STRING,
    age INT,
    salary DOUBLE,
    is_active BOOLEAN,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'ytsaurus',
    'clusterName' = 'localhost:8082',
    'path' = '//tmp/flink_simple_test_table',
    'credentialsSource' = 'options',
    'ytUsername' = 'admin',
    'ytToken' = 'password',
    'schema' = '[
        {"name"="id";"type"="int64";"required"=%false;"sort_order"="ascending"};
        {"name"="name";"type"="string";"required"=%false};
        {"name"="age";"type"="int64";"required"=%false};
        {"name"="salary";"type"="double";"required"=%false};
        {"name"="is_active";"type"="boolean";"required"=%false};
        {"name"="created_at";"type"="string";"required"=%false};
        {"name"="updated_at";"type"="string";"required"=%false}
    ]'
);
```

3) Run job that writes generated data to YTsaurus dynamic table

```SQL
INSERT INTO ytsaurus_simple_sink
SELECT
    id,
    name,
    age,
    salary,
    is_active,
    created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM simple_datagen_source;
```

4) You can watch your job progress at [localhost:8081](http://localhost:8081)

![](../assets/images/ytsaurus_simple_sink_job_ui.png)

5) In the `/tmp/flink_simple_test_table` directory, a table `flink_simple_test_table` will be created that contains the results of the Flink job.

![](../assets/images/ytsaurus_simple_sink_result_ui.png)

Congratulations! You've launched your first job with YTsaurus and Apache Flink.
