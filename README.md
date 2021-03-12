# varstream
> Streaming VAR with Flink SQL

Demonstrating the power of Flink SQL for stream processing of market data. The SQL scripts in this
project make use of custom UDFs. See below for instructions on how to build.

## Build UDFs

```sh
git clone https://github.com/patrickangeles/varstream
cd varstream
mvn clean package
```

## Using sql-client with UDFs

```sh
$FLINK_HOME/bin/sql-client.sh embedded --jar target/varstream-0.1-SNAPSHOT.jar
```

## License

Distributed under the ASLv2 license. See ``LICENSE`` for more information.
