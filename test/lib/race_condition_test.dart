library dart_cassandra_cql.tests.race_condition;

import "dart:async";
import "package:unittest/unittest.dart";
import 'package:dart_cassandra_cql/dart_cassandra_cql.dart' as cql;

main() {
  group("Race Conditions:", () {
    cql.Client client;
    setUp(() async {
      client = new cql.Client.fromHostList(['127.0.0.1:9042']);
      await client.execute(new cql.Query('''
        CREATE KEYSPACE IF NOT EXISTS cassandra_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
      '''));
      await client.execute(new cql.Query('''
        CREATE TABLE IF NOT EXISTS cassandra_test.test_table (
            id int,
            data varchar,
            PRIMARY KEY (id)
        )
        WITH caching = '{"keys":"NONE", "rows_per_partition":"NONE"}'
      '''));
    });

    tearDown(() {
      return client.connectionPool.disconnect(drain: false);
    });

    test("it can handle execution of multiple queries scheduled synchronously",
        () {
      var f1 = client
          .execute(new cql.Query('SELECT * FROM cassandra_test.test_table'));
      var f2 = client
          .execute(new cql.Query('SELECT * FROM cassandra_test.test_table'));

      expect(Future.wait([f1, f2]), completes);
    });
  });
}
