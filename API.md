# Api

## Cassandra to dart type mapping

The following table summarizes type mappings between Cassandra and dart\_cassandra\_cql.  Due to the fact we cannot establish a one-to-one mapping between the Dart and Cassandra types, the following conversions are only applied when:

 - the driver parses query responses from the server
 - the driver encodes data prior to execution of a [prepared](#prepared-queries) query

In all other cases, the driver performs *automatic inline expansion* of each bound query parameter.

|Cassandra type | Dart type
|:------------- |:---------
|ascii          | String
|bigint         | int
|blob           | [Uint8List](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart-typed_data.Uint8List)
|boolean         | bool
|counter        | int
|decimal        | int or double
|double         | double
|float          | double
|inet           | [InternetAddress](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:io.InternetAddress)
|int            | int
|list&lt;X>      |  List&lt;X>
|map&lt;X, Y>    | LinkedHashMap&lt;X, Y>
|set&lt;X>       | Set&lt;X>
|text           | String
|timestamp      |  [DateTime](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:core.DateTime)
|uuid           | [Uuid](#uuids)
|timeuuid       | [Uuid](#uuids)
|varchar        | String
|varint         | BigInt
|UDT            | LinkedHashMap. See the section on [UDTs](#user-defined-types)
|tuple          | [Tuple](#tuples)
|custom         | [Uint8List](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart-typed_data.Uint8List) or type instance implementing [CustomType](#custom-types)

### UUIDs

The [Uuid](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/types/uuid.dart) class provides a wrapper around UUIDs and provides factory constructors for generating simple and time-based UUIDs.

```dart
Uuid simpleUuid = Uuid.simple();
Uuid timeUuid = Uuid.timeBased();
```

If you have some externally generated UUIDs that you wish to pass to a Cassandra query you can either pass them as a ```String``` or wrap them with a Uuid object:

```dart
Uuid externalUuid = new Uuid("550e8400-e29b-41d4-a716-446655440000");
```

### User defined types

The driver supports UDTs with arbitrary nesting. The driver will parse UDTs as ```LinkedHashMap<String, Object>``` objects.

### Tuples

Whenever you need to use a ```tuple``` type in your queries or read it from a query result you need to use the driver-provided [Tuple](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/types/tuple.dart) class.

This class is essentially a decorated ```List<Object>```. You can instanciate a ```Tuple``` object from any ```Iterable``` using the ```fromIterable``` named constructor. Here is an example:

```dart
Tuple tuple = Tuple.fromIterable([1, "test", 3.14]);
```

### Custom types

Custom types are user-defined server-side Java classes that extend the types supported by Cassandra. These classes provide mechanisms for handing custom type serialization/de-serialization.

By *default*, custom types are parsed and returned as a [Uint8List](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart-typed_data.Uint8List). The driver however allows you to register a user-defined [Codec](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart-convert.Codec) for handling custom type serialization/de-serialization to a *Dart class instance*.

To use this feature, the Dart class that represents the custom type needs to implement the [CustomType](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/types/custom_type.dart) interface:

```dart
abstract class CustomType {
  String get customTypeClass;
}
```

This interface defines a *getter* for querying the fully qualified Java class name that implements this custom type at the server and is used by the driver to select the appropriate ```Codec``` when it encounters an instance of this class.

You will also need to register a ```Codec<Uint8List, CustomType>``` for handing the actual serialization/de-serialization. To register the codec you will need to invoke the globally available ```registerCodec``` method as follows:

```dart
registerCodec('fully.qualified.java.class.name', MyCustomTypeCodec() );
```

After this step, the driver will automatically invoke the codec whenever it encounters a custom type with this class name while parsing query results or whenever an ```CustomType``` object instance of this type is bound to a query.

## The connection pool

A connection pool is used to keep track of active connections to Cassandra nodes and to provide load-balancing, fault-tolerance and server event subscription to Cassandra clients.

To instanciate a connection pool you need to provide a [PoolConfiguration](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/connection/pool_configuration.dart) object to the pool constructor. The following named parameters may be passed to the PoolConfiguration object constructor to override the default values:

|Option name              | Default value           | Description   |
|:------------------------|:------------------------|:---------------
| protocolVersion         | ProtocolVersion.V2  | The binary protocol version to use. Its value should be one of the [ProtocolVersion](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/types/enums/protocol_version.dart) enums
| cqlVersion              | "3.0.0"             | The CQL version to use. To use the new CQL features you should request at least version **"3.1.0"**
| connectionsPerHost      | 1                   | The number of concurrent connections to each host
| streamsPerConnection    | 128                 | The max number of requests that can be multiplexed over each connection. According to the binary protocol specification, each connection can multiplex up to **128** streams in V2 mode and **32768** streams in V3 mode
| maxConnectionAttempts   | 10                  | The max number of reconnection attempts before declaring a connection as unusable
| reconnectWaitTime       | 1500ms              | A [Duration](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:core.Duration) object specifying the time to wait between reconnection attempts
| streamReservationTimeout| 0ms                 | A [Duration](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:core.Duration) object specifying the time to wait for a connection stream to become available when all connection streams are in use. In case of a timeout, the driver will try the next available connection
| autoDiscoverNodes       | true                | If this flag is set to ```true```, the connection pool will listen for server topology change events and automatically update the pool when new nodes come online/go offline. If set to ```false```, the pool will only process UP/DOWN events for nodes already in the pool
| authenticator           | null                | An authentication provider instance implementing the [Authenticator](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/protocol/authentication/authenticator.dart) interface or **null** if no authentication is required. For more information see the section on [authentication](#cassandra-authentication)
| compression             | null                | The compression algorithm to be used with Cassandra nodes. Its value should be one of the [Compression](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/types/enums/compression.dart) enums or **null** if no compression should be used. For more information see the section on [compression](#compression)
| preferBiggerTcpPackets  | false               | Join together frame data before piping them to the underlying TCP socket. Enabling this option will improve performance at the expense of slightly higher memory consumption

### Simple connection pool

The driver provides the [SimpleConnectionPool](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/connection/simple_connection_pool.dart) as its default [ConnectionPool](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/connection/connection_pool.dart) implementation. This pool should be adequate for most projects although you can always roll up your [own pool](#custom-connection-pools) if you want.

The pool manages a list of ```connectionsPerHost``` open connections and
tries to load-balance between them. In addition to that, the pool subscribes to server generated events and can add/remove connections from the pool whenever Cassandra nodes come online/go offline or nodes enter/exit the cluster.

If the ```autoDiscoverNodes``` pool configuration option is set to ```true``` then the pool will automatically open connections to *nodes not originally present* in the pool when they come online.

#### Connection selection

The simple connection pool uses the following algorithm for selecting connections:

- Select the first healthy connection with available connection streams.
- If no connection satisfies these criteria, then the first healthy connection with no available connection streams is selected.

In the later case, the driver will attempt to reserve a connection stream. The ```streamReservationTimeout``` pool configuration option controls the reservation timeout. If the reservation times out then another connection is selected from the pool and the process is repeated.

In any case, whenever a connection is selected from the pool, it will be removed from its current position in the pool and moved to its end. This allows the driver to perform load-balancing.

### Custom connection pools

To create a custom connection pool you need to extend the
abstract [ConnectionPool](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/connection/connection_pool.dart) class and implement its abstract methods to apply your custom connection selection logic. An instance of your custom connection pool can then be passed to the [client](#the-client) during construction. Here are a few ideas for a custom connection pool:

- A DC-aware connection pool that tries local DC connections first and then falls back to a secondary, tertiary e.t.c connection pool.
- A connection pool that discovers Cassandra hosts automatically via an external service (EC2 API queries, etcd)

## Cassandra authentication

The driver provides the built-in [PasswordAuthenticator](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/protocol/authentication/password_authenticator.dart) class that allows you to authenticate with Cassandra servers requiring the ```org.apache.Cassandra.auth.PasswordAuthenticator``` provider. To use this provider, you need to instanciate it with your user/pass credentials and pass it to the pool configuration.


If you need to implement any other form of authentication you can roll your own by implementing the [Authenticator](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/protocol/authentication/authenticator.dart) interface.

```dart
abstract class Authenticator {
  String get authenticatorClass;

  Uint8List answerChallenge(Uint8List challenge);
}
```

The implementation should define:
+ a **getter** for retrieving the *fully qualified Cassandra authentication class* that the authenticator handles. This value is used to match the authenticator class requested by Cassandra during the initial handshake.
+ an **answerChallenge** method that will be invoked each time the remote server sends an authentication challenge.

If the Cassandra cluster requires authentication but none is supplied or an incompatible authenticator instance is specified, then connection attempts will fail with an [AuthenticationException](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/exception/authentication.dart) error.

## Compression

In order to keep external dependencies to a minimum, the driver does not ship with native implementations for the two compression schemes currently supported by Cassandra (lz4 and snappy).

The driver however allows provides a mechanism for registering a [Codec\<Uint8List, Uint8List>](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:convert.Codec) that implements one of the above compression schemes. This allows you to use a third-party dart package (e.g [dart_lz4](https://github.com/achilleasa/dart_lz4) native extension) for handling compression if your particular application requires it.

To use this feature you need to invoke the public method ```registerCodec(String, Codec<Object, Uint8List>)``` and specify one of the [Compression](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/types/enums/compression.dart) enum **values** (e.g. ```Compression.LZ4.value```) as the first argument and a class instance implementing ```Codec<Uint8, Uint8>``` as the second argument.

After the codec is registered you may then enable compression when you define your pool configuration. Keep in mind that the codec needs to be registered **before** the pool configuration is instanciated or an exception will be thrown.

Here is an example for registering a codec and requesting it when creating the client:

```dart
import "package:dart_cassandra_cql/dart_cassandra_cql.dart" as cql;
import "mocks/lz4.dart" as compress;

int main() {
  cql.registerCodec(cql.Compression.LZ4.value, compress.LZ4Codec());

  // This client will now use LZ4 compression when talking to Cassandra
  cql.Client client = cql.Client.fromHostList(
      ['10.0.0.1']
      , poolConfig : cql.PoolConfiguration(
          protocolVersion : cql.ProtocolVersion.V2
          , compression : cql.Compression.LZ4
      )
  );
}
```

## The client

To create a new client instance you may use one of the two available named constructors:

- ```Client.fromHostList(List<String> hosts, {String defaultKeyspace, PoolConfiguration poolConfig})``` which should be used when a host list is available. You may also specify the default keyspace to be used as well as a specific pool configuration (if no configuration is specified a default one will be used). This method will instanciate a [SimpleConnectionPool](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/connection/simlpe_connection_pool.dart) with the supplied parameters and bind it to the client.
- ```Client.withPool(ConnectionPool this.connectionPool, {String defaultKeyspace})``` which should be used with an already instanciated connection pool (simple or custom).

### Single and batch queries

To define a single query, the driver provides the [Query](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/query.dart) class. Its constructor accepts a query string that may contain either positional (indicated by the ```?``` character) or named (indicated by ```:``` followed by the parameter name) arguments. Mixing positional and named parameters is not currently supported. The following named parameters may also be passed to the constructor:

| Parameter Name | Description
|:---------------|:----------------
| bindings       | The query string bindings. The parameter value value must be a ```Iterable<String>``` when using positional parameters or a ```Map<String, Object>``` when  using named parameters
| consistency    | One of the [Consistency](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/types/enums/consistency.dart) enum values to select the consistency level for this query. Defaults to ```Consistency.QUORUM```
| serialConsistency | One of ```Consistency.SERIAL```, ```Consistency.LOCAL_SERIAL``` or ```Consistency.LOCAL_ONE``` enum values to select the serial consistency value of this call. This value will be ignored when not using ```ProtocolVersion.V3```
| prepared       | A boolean flag specifying whether this query should be prepared or not. Defaults to ```false```. For more info see the section on [prepared queries](#prepared-queries)

Here are some examples:

```dart
cql.Query(
  "SELECT * FROM test.test_table WHERE id=? AND alt_id=?"
  , bindings : [ 1, 2 ]
);

cql.Query(
  "SELECT * FROM test.test_table WHERE id=:id AND alt_id=:id"
  , bindings : { "id" : 1 }
  , consistency : cql.Consistency.ONE
);
```

If you need to execute a batch query, the driver provides the [BatchQuery](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/batch_query.dart) class. The class constructor accepts the following optional params:

| Parameter Name | Description
|:---------------|:----------------
| consistency    | One of the [Consistency](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/types/enums/consistency.dart) enum values to select the consistency level for the batch query. Defaults to ```Consistency.QUORUM```.
| serialConsistency | One of ```Consistency.SERIAL```, ```Consistency.LOCAL_SERIAL``` or ```Consistency.LOCAL_ONE``` enum values to select the serial consistency value of this call. This value will be ignored when not using ```ProtocolVersion.V3```.
| batchType       | One of the [BatchType](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/types/enums/batch_type.dart) enum values to select the batch type. Defaults to ```BatchType.LOGGED```.  This value will be ignored when not using ```ProtocolVersion.V3```.

The ```BatchQuery``` class provides the ```add( Query query)``` method for appending individual ```Query``` instances to the batch. Keep in mind that when useing batch queries, the ```consistency``` and ```serialConsistency``` settings of the ```BatchQuery``` object override any individual consistency settings specified by the appended ```Query``` objects. Here is an example:

```dart
cql.BatchQuery(consistency: cql.Consistency.TWO)
  ..add(cql.Query(
    "INSERT INTO test.test_table (id, alt_id) VALUES (?, ?)"
    , bindings : [ 1, 2 ]
  ))
  ..add(cql.Query(
    "INSERT INTO test.test_table (id, alt_id) VALUES (:id, :id)"
    , bindings : {"id" : 1}
  ));
```


### Executing queries

The client provides two methods for executing single queries: ```query()``` and ```execute()```.

To execute a single **select** [Query](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/query.dart), the ```query``` method should be used. It returns back a [Future](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:async.Future) that evaluates to an ```Iterable<Map<String, Object>>``` with the result rows. Each row is modeled as a ```Map<String, Object>``` where the key is the column name and the value is the [unserialized](#cassandra-to-dart-type-mapping) column value.

In all other cases (single queries or batch queries) the ```execute``` method should be used. This method accepts either a [Query](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/query.dart) or [BatchQuery](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/batch_query.dart) as its argument. The method also accepts the optional ```pageSize``` and ```pagingState``` named parameters to enable [pagination](#paginated-queries) for single select queries. It returns back a [Future](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:async.Future) that evaluates to one of the following concrete implementations of the [ResultMessage](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/protocol/messages/responses/result_message.dart) class depending on the query type:

|Message type               | Returnes when |
|:--------------------------|:---------------
| [VoidResultMessage](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/protocol/messages/responses/void_result_message.dart)        | If the query returns no value. (e.g. ```insert``` queries)
| [RowsResultMessage](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/protocol/messages/responses/rows_result_message.dart)         | When executing a ```select``` query.
| [SetKeyspaceResultMessage](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/protocol/messages/responses/set_keyspace_result_message.dart)  | When a ```set keyspace``` query is executed.
| [SchemaChangeResultMessage](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/protocol/messages/responses/schema_change_result_message.dart) | When an ```alter``` query is executed.

### Paginated queries

In some use cases, you may need to perform client-side pagination. One way to achieve this is to encode the pagination parameters (e.g. fromDate - toDate) inside the ```where``` clause of your selection query. Another way to achieve this is to use Cassandra's native pagination support.

To use native pagination you need to invoke the ```execute``` method with your selection query and supply the ```pageSize``` named parameter. After the returned [Future](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:async.Future) completes, you receive a [RowsResultMessage](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/protocol/messages/responses/rows_result_message.dart) . You can access the returned rows (a ```List<Map<String, Object>>```) via the ```rows``` getter. The message also contains a [ResultMetadata](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/protocol/messages/responses/result_metadata.dart) instance which can be obtained via the ```metadata``` getter. The metadata object contains the ```pagingState``` attribute whose value is generated by Cassandra and serves as a pointer for obtaining the next page of results.

To retrieve the next set of rows you need to invoke once again the ```execute``` method with the same ```pageSize``` value as before and the ```pagingState``` named parameter set to the value obtained by the previous method invocation. Here is an example:

```dart
cql.Query query = cql.Query("SELECT * from really_big_dataset");
client
  .execute(query, pageSize : 10)
  .then((cql.RowsResultMessage result) {
      print("Page 1");
      print(result.rows);

      // Fetch next page
      return client.execute(query, pageSize : 10, pagingState : result.metadata.pagingState);
  })
  .then((cql.RowsResultMessage result) {
	  print("Page 2");
	  print(result.rows);
  });
```

### Streaming query results

If you need to iterate through all rows of a large dataset without loading the entire dataset to memory you can use the ```stream``` method. This method accepts a [Query](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/query.dart) object and the named parameter ```pageSize``` (defaults to 100) and returns back a Dart [Stream](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:async.Stream). The stream supports the usual stream-related operations (pause, resume, stop e.t.c) and can be combined with other
Dart streams for additional processing.

The underlying StreamController buffers the rows for each page on-demand and emits each row to the stream as a ```Map<String, Object>``` where the key is the column name and the value is the [unserialized](#cassandra-to-dart-type-mapping) column value. Here is an example that streams all rows from a dataset and prints each one to the console:

```dart
client.stream(
       cql.Query("SELECT * FROM really_big_dataset")
       , pageSize: 20
).listen( (Map<String, Object> row) => print );
```

### Prepared queries

If you are executing the same single [Query](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/query.dart) multiple times you can increase your query throughput by converting it to a prepared query. To do this, set the ```prepared``` named constructor parameter to ```true``` when you instanciate your query object. When executing a prepared query, the driver is aware of the actual Cassandra type of each bound argument and will properly serialize the bound arguments instead of performing _automatic inline expansion_ as with normal queries.

Prepared queries are associated with a randomly selected node from the connection pool and they can only be executed via connections to that particular node. If during execution time no connection to the associated node is available, the driver will automatically prepare the query on another random node from the pool and execute it there.

### Listening for Cassandra events

The connection pool registers itself as a listener for events broadcasted by Cassandra nodes. These events include notifications about:
- schema changes (keyspace/table create, update, drop)
- node status changes (node came up or went down)
- cluster topology changes (node added or removed)

The connection pool uses these events to add new nodes to the pool (if the ```autoDiscoverNodes``` pool configuration option is true) to remove dead nodes from the pool or to attempt to reconnect to offline nodes when they go online.

If your application needs to process these kinds of events, you can use the ```listenForServerEvents``` method of the connection pool. This method accepts a [List](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:core.List) of [EventRegistrationType](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/types/enums/event_registration_type.dart) values and returns back a Dart [Stream](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:async.Stream) that emits [EventMessage](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/protocol/messages/responses/event_message.dart) objects. Here is an example:

```dart
client.connectionPool.listenForServerEvents([
    cql.EventRegistrationType.SCHEMA_CHANGE
]).listen( (cql.EventMessage message ){
  print("Got a ${message.type} event with sub type ${message.subType} for keyspace ${message.keyspace}");
});
```

### Shutting down the client

The shutdown the client you need to invoke the ```shutdown()``` method.
This method accepts the named parameters ```drain``` (defaults to ```true```) and ```drainTimeout``` (defaults to ```Duration(seconds : 5)``` that control how the shutdown should be performed and returns a [Future](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:async.Future).

To perform a graceful shutdown, set the ```drain``` named parameter to ```true```. The client will mark all connections as unhealthy so that no more queries can be performed and wait for any pending queries to complete (or the ```drainTimeout``` expires) before shutting down each connection. The returned [Future](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:async.Future) will complete once all active connections shut down.

If the ```drain``` named parameter is set to ```false``` then the client will mark all connections as unhealthy so that no more queries can be performed and *abort* any pending queries.


### Handling errors

Whenever you invoke one of the available client methods, you should always watch out for errors as unhandled errors will probably terminate your application or current zone.

For more granular error handling you can test for the following exception types:

| Exception type                | Thrown
|:------------------------------|:-------------------
| [NoHealthyConnectionsException](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/exception/no_healthy_connections_exception.dart) | When the connection pool contains no healthy connections
| [AuthenticationException](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/exception/authentication_exception.dart)       | When authentication failed, missing Authentication provider or unsupported Authentication provider
| [CassandraException](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/exception/cassandra_exception.dart)            | When cassandra reports an exception (invalid query, cannot meet consistency requirement e.t.c)
| [DriverException](https://github.com/achilleasa/dart_cassandra_cql/blob/master/lib/src/exception/driver_exception.dart)               | General driver exception

All of the above exceptions include a ```message``` getter for retrieving the exception cause and a ```stackTrace``` getter for accessing the stack trace (if available).

