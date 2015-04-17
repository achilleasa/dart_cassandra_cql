# Dart Driver for Apache Cassandra

[![Build Status](https://drone.io/github.com/achilleasa/dart_cassandra_cql/status.png)](https://drone.io/github.com/achilleasa/dart_cassandra_cql/latest)
[![Coverage Status](https://coveralls.io/repos/achilleasa/dart_cassandra_cql/badge.svg)](https://coveralls.io/r/achilleasa/dart_cassandra_cql)

Dart driver for [Apache Cassandra](http://Cassandra.apache.org/) that supports Cassandra Query Language version [3.0+](http://www.datastax.com/documentation/cql/3.1/cql/cql_intro_c.html) (CQL3). 

The driver has a small dependency tree and implements Cassandra binary protocol (versions 2 and 3) for communicating with Cassandra servers. The protocol and CQL versions to be used are both configurable by the user.

# Features
 - Asynchronous API based on [Future](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart:async.Future) and [Streams](https://api.dartlang.org/apidocs/channels/stable/dartdoc-viewer/dart-async.Stream)
 - Connection management via connection pools
 - Connection load-balancing and failover
 - Server event handling (node/topology/schema change events)
 - Query multiplexing on each connection
 - Batch and prepared queries with either positional or named placeholders
 - Query result streaming
 - Support for all Cassandra types including [user defined types](http://www.datastax.com/dev/blog/cql-in-2-1) (UDT),  [tuples](http://www.datastax.com/documentation/developer/java-driver/2.1/java-driver/reference/tupleTypes.html) and custom types (via user-defined Codecs)

# Quick start

```dart
import "dart:async";
import 'package:dart_cassandra_cql/dart_cassandra_cql.dart' as cql;

void main() {
  // Create a client for connecting to our cluster using native
  // protocol V2 and sensible defaults. The client will setup
  // a connection pool for you and connect automatically when
  // you execute a query.
  cql.Client client = new cql.Client.fromHostList([
      "10.0.0.1:9042"
      , "10.0.0.2:9042"
  ]);

  // Perform a select with positional bindings
  client.query(
      new cql.Query("SELECT * from test.type_test WHERE id=?", bindings : [123])
  ).then((Iterable<Map<String, Object>> rows) {
    // ...
  });

  // Perform an prepared insert with named bindings, a time-based uuid and tuneable consistency
  client.execute(
      new cql.Query("INSERT INTO test.type_test (id, uuid_value) VALUES (:id, :uuid)", bindings : {
          "id" : 1
          , "uuid" : new cql.Uuid.timeBased()
      }, consistency : cql.Consistency.LOCAL_QUORUM
       , prepared : true)
  ).then((cql.ResultMessage res) {
    // ...
  });

  // Perform a batch insert query
  client.execute(
      new cql.BatchQuery()
        ..add(
          new cql.Query("INSERT INTO test.type_test (id, uuid_value) VALUES (:id, :uuid)", bindings : {
              "id" : 1
              , "uuid" : new cql.Uuid.timeBased()
          })
      )
        ..add(
          new cql.Query("INSERT INTO test.type_test (id, uuid_value) VALUES (:id, :uuid)", bindings : {
              "id" : 2
              , "uuid" : new cql.Uuid.timeBased()
          })
      )
        ..consistency = cql.Consistency.TWO
  ).then((cql.ResultMessage res) {
    // ...
  }).catchError((e) {
    // Handle errors
  });

  // Stream (paginated) query
  StreamSubscription sub;
  sub = client.stream(
      new cql.Query("SELECT * from test.type_test")
      , pageSize : 200
  ).listen((Map<String, Object> row) {
    // Handle incoming row
    print("Next row: ${row}");
    // ... or manipulate stream
    sub.cancel();
  });

}
```

# Api

See the [Api documentation](https://github.com/achilleasa/dart_cassandra_cql/blob/master/API.md).


# Contributing

See the [Contributing Guide](https://github.com/achilleasa/dart_cassandra_cql/blob/master/CONTRIBUTING.md).

# Acknowledgements

- The design and implementation of this driver borrows lots of ideas from [node-cassandra-cql](https://github.com/jorgebay/node-cassandra-cql/). 
- The varint and decimal type decoders have been ported from the [DataStax cpp driver](https://github.com/datastax/cpp-driver).

# License

dart\_cassandra\_cql is distributed under the [MIT license](https://github.com/achilleasa/dart_cassandra_cql/blob/master/LICENSE).

