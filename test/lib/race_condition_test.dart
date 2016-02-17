library dart_cassandra_cql.tests.race_condition;

import "dart:async";
import "package:unittest/unittest.dart";
import 'package:dart_cassandra_cql/dart_cassandra_cql.dart' as cql;
import "mocks/mocks.dart" as mock;

main({bool enableLogger: true}) {
  if (enableLogger) {
    mock.initLogger();
  }

  const String SERVER_HOST = "127.0.0.1";
  const int SERVER_PORT = 32000;
  mock.MockServer server = new mock.MockServer();

  group("Race Conditions:", () {
    setUp(() {
      return server.listen(SERVER_HOST, SERVER_PORT);
    });

    test("it can handle execution of multiple queries scheduled synchronously",
        () {
      server.setReplayList(["select_v2.dump", "select_v2.dump"]);
      var client = new cql.Client.fromHostList(
          ["${SERVER_HOST}:${SERVER_PORT}"],
          poolConfig: new cql.PoolConfiguration(autoDiscoverNodes: false));
      var f1 = client.execute(new cql.Query('SELECT * from test.type_test'));
      var f2 = client.execute(new cql.Query('SELECT * from test.type_test'));

      expect(Future.wait([f1, f2]), completes);
    });
  });
}
