library dart_cassandra_cql.tests.pool_config;

import "package:test/test.dart";

import '../../lib/dart_cassandra_cql.dart' as cql;

main({bool enableLogger: true}) {
  group("Pool config:", () {
    test("invalid protocol", () {
      expect(
          () => new cql.PoolConfiguration(protocolVersion: null),
          throwsA(predicate((ex) =>
              ex is ArgumentError &&
              ex.message == "Driver only supports protocol versions 2 and 3")));
    });

    test("invalid streams per connection (V2 protocol)", () {
      expect(
          () => new cql.PoolConfiguration(
              protocolVersion: cql.ProtocolVersion.V2,
              streamsPerConnection: 1024),
          throwsA(predicate((ex) =>
              ex is ArgumentError &&
              ex.message ==
                  "Invalid value for option 'streamsPerConnection'. Expected a value between 1 and 128 when using V2 prototcol")));
    });

    test("invalid protocol", () {
      expect(
          () => new cql.PoolConfiguration(
              protocolVersion: cql.ProtocolVersion.V3,
              streamsPerConnection: 65536),
          throwsA(predicate((ex) =>
              ex is ArgumentError &&
              ex.message ==
                  "Invalid value for option 'streamsPerConnection'. Expected a value between 1 and 3768 when using V3 prototcol")));
    });
  });
}
