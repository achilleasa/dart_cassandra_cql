library dart_cassandra_cql.tests.connection;

import "dart:io";
import "dart:async";
import "package:unittest/unittest.dart";
import "mocks/mocks.dart" as mock;
import "mocks/compression.dart" as compress;
import '../../lib/dart_cassandra_cql.dart' as cql;
import '../../lib/driver/exceptions.dart' as cqlEx;

main({bool enableLogger : true}) {
  if( enableLogger ){
    mock.initLogger();
  }

  const String SERVER_HOST = "127.0.0.1";
  const int SERVER_PORT = 32000;
  mock.MockServer server = new mock.MockServer();
  cql.Connection conn;

  group("Connection", () {

    setUp(() {
      conn = null;
      return server.listen(SERVER_HOST, SERVER_PORT);
    });

    tearDown(() {
      server.setCompression(null);
      List<Future> cleanupFutures = [
          server.shutdown()
      ];
      if (conn != null) {
        cleanupFutures.add(conn.close(drain : false));
      }

      return Future.wait(cleanupFutures);
    });

    test("frame parsing exception should be wrapped in an ExceptionMessage", () {
      cql.PoolConfiguration config = new cql.PoolConfiguration(
          protocolVersion : cql.ProtocolVersion.V3
      );
      // We have modified this event message to use stream id 0 and
      // include a malformed INET field. This should trigger an exception when we try to parse it
      server.setReplayList(["malformed_frame_v3.dump"]);
      conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);

      void handleError(e, trace) {
        expect(e, new isInstanceOf<Exception>());
        expect(e.message, equals("Could not decode INET type of length 6"));
        expect(trace, isNotNull);
      }
      conn.open()
      .then((_) => conn.execute(new cql.Query("SELECT * FROM test")))
      .catchError(expectAsync(handleError));
    });

    test("failure with reconnect attempts", () {
      cql.PoolConfiguration config = new cql.PoolConfiguration(
          protocolVersion : cql.ProtocolVersion.V2
          , maxConnectionAttempts : 2
          , reconnectWaitTime : new Duration(milliseconds : 1)
      );
      conn = new cql.Connection("conn-0", SERVER_HOST, 123, config : config);

      void handleError(e) {
        expect(e, new isInstanceOf<cqlEx.ConnectionFailedException>());
      }
      conn.open().catchError(expectAsync(handleError));
    });

    test("stream reservation timeout exception", () {
      cql.PoolConfiguration config = new cql.PoolConfiguration(
          protocolVersion : cql.ProtocolVersion.V2
          , streamReservationTimeout : new Duration(milliseconds : 10)
          , streamsPerConnection : 1
      );
      conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);

      void handleError(e) {
        expect(e, new isInstanceOf<cqlEx.StreamReservationException>());
        expect(e.toString(), startsWith("StreamReservationException"));
      }
      conn.open()
      .then((_) {
        // Future f1 will grab our stream writer and wait for a server connection
        Future f1 = conn.execute(new cql.Query("SELECT * FROM test")).catchError((_) {
        });
        // With a small delay, try a second query which should fail with a reservation timeout exception
        return new Future.delayed(new Duration(milliseconds:10), () => conn.execute(new cql.Query("SELECT * FROM test")));
      }).catchError(expectAsync(handleError));
    });

    test("Multiple connection attempts", () {
      cql.PoolConfiguration config = new cql.PoolConfiguration(protocolVersion : cql.ProtocolVersion.V2);
      conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);
      Future f1 = conn.open();
      Future f2 = conn.open();
      expect(f1, equals(f2));

      return f1;
    });

    test("V2 handshake", () {
      cql.PoolConfiguration config = new cql.PoolConfiguration(protocolVersion : cql.ProtocolVersion.V2);
      conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);
      return conn.open();
    });

    test("V2 handshake (using default pool configuration)", () {
      conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT);
      return conn.open();
    });

    test("V2 handshake and keyspace selection", () {
      server.setReplayList(["void_result_v2.dump"]);
      cql.PoolConfiguration config = new cql.PoolConfiguration(protocolVersion : cql.ProtocolVersion.V2);
      conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);
      return conn.open();
    });

    test("V3 handshake", () {
      cql.PoolConfiguration config = new cql.PoolConfiguration(protocolVersion : cql.ProtocolVersion.V3);
      conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);
      return conn.open();
    });

    test("lost exception", () {
      cql.PoolConfiguration config = new cql.PoolConfiguration(
          protocolVersion : cql.ProtocolVersion.V3
          , maxConnectionAttempts : 1
      );
      conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);

      void handleError(e) {
        expect(e, new isInstanceOf<cqlEx.ConnectionFailedException>());
      }

      conn.open()
      .then((_) => server.shutdown())
      .then((_) => conn.execute(new cql.Query("SELECT * FROM foo")))
      .catchError(expectAsync(handleError));
    });

    test("default keyspace (V2)", () {
      server.setReplayList(["set_keyspace_v2.dump"]);
      cql.PoolConfiguration config = new cql.PoolConfiguration(protocolVersion : cql.ProtocolVersion.V3);
      conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config, defaultKeyspace : "test");

      expect(conn.open(), completes);
    });

    test("query (V2)", () {
      server.setReplayList(["select_v2.dump"]);
      cql.PoolConfiguration config = new cql.PoolConfiguration(protocolVersion : cql.ProtocolVersion.V3);
      conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);
      expect(
          conn.open().then((_) {
            return conn.execute(new cql.Query("SELECT * from test.type_test"));
          }),
          completion((cql.RowsResultMessage res) {
            expect(res.rows.length, equals(1));
            Map<String, Object> row = res.rows.first;
            Map<String, Object> expectedValues = {
                "ascii_type" : "text4"
                , "bigint_type" : 9223372036854775807
                , "bool_type" : true
                , "inet_type" : new InternetAddress("192.168.169.102")
                , "int_type" : 32238493
                , "list_type" : [100, 200]
                , "map_type" : {
                    100 : "the test"
                    , 200 : "the result"
                }
                , "set_type" : [100, 200]
                , "text_type" : "This is a long UTF8 κείμενο"
                , "uuid_type" : new cql.Uuid("550e8400-e29b-41d4-a716-446655440000")
                , "varchar_type" : "Arbitary long text goes here"
                , "varint_type" : -3123091212904812093120938120938120312890
            };
            expectedValues.forEach((String fieldName, Object fieldValue) {
              expect(row[fieldName], equals(fieldValue));
            });
            return true;
          })
      );
    });

    test("query (V3)", () {
      server.setReplayList(["select_v3.dump"]);
      cql.PoolConfiguration config = new cql.PoolConfiguration(protocolVersion : cql.ProtocolVersion.V3);
      conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);
      expect(
          conn.open().then((_) {
            return conn.execute(new cql.Query("SELECT * FROM test.user_profiles"));
          }),
          completion((cql.RowsResultMessage res) {
            expect(res.rows.length, equals(1));
            Map<String, Object> row = res.rows.first;
            Map<String, Object> expectedValues = {
                "login": "test_user"
                , "addresses": {
                    "home": {
                        "street": "123 Test Str."
                        , "city": "San Fransisco"
                        , "zip": 94110
                        , "phones":[
                            {
                                "number": "123 444 5555"
                                , "tags": [
                                "direct line"
                                , "preferred"
                            ]
                            },
                            {
                                "number": "123 444 6666"
                                , "tags": ["fax"]
                            }
                        ]
                    }
                }
                , "email": "tuser@test.com"
                , "first_name": "Test"
                , "last_name": "User"
            };
            expectedValues.forEach((String fieldName, Object fieldValue) {
              expect(row[fieldName], equals(fieldValue));
            });
            return true;
          })
      );

    });

    group("with compression:", () {
      group("mock-SNAPPY:", () {
        setUp(() {
          server.setCompression(cql.Compression.SNAPPY);
        });

        tearDown(() {
          cql.unregisterCodec(cql.Compression.LZ4.value);
          cql.unregisterCodec(cql.Compression.SNAPPY.value);
        });

        test("Codec encode exception handling", () {
          // Register codec for the mock server
          cql.registerCodec(cql.Compression.SNAPPY.value, new compress.MockCompressionCodec(true));

          cql.PoolConfiguration config = new cql.PoolConfiguration(
              protocolVersion : cql.ProtocolVersion.V2
              , compression : cql.Compression.SNAPPY
          );

          conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);

          void handleError(e) {
            expect(e.message, startsWith("An error occurred while invoking '${cql.Compression.SNAPPY}' codec (compression)"));
          }

          conn.open()
          .then((_) => conn.execute(new cql.Query("SELECT * from test.type_test")))
          .catchError(expectAsync(handleError));
        });

        test("server responds with unexpected compressed frame", () {
          server.setReplayList(["void_result_v2.dump"]);

          // Register codec for the mock server
          cql.registerCodec(cql.Compression.SNAPPY.value, new compress.MockCompressionCodec());

          cql.PoolConfiguration config = new cql.PoolConfiguration(
              protocolVersion : cql.ProtocolVersion.V2
          );

          conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);

          void handleError(e) {
            expect(e.message, equals("Server responded with an unexpected compressed frame"));
          }

          conn.open()
          .then((_) => conn.execute(new cql.Query("SELECT * from test.type_test")))
          .catchError(expectAsync(handleError));
        });

        test("server responds with compressed frame but we have unregistered the codec", () {

          // Register codec for the mock server
          cql.registerCodec(cql.Compression.SNAPPY.value, new compress.MockCompressionCodec());

          cql.PoolConfiguration config = new cql.PoolConfiguration(
              protocolVersion : cql.ProtocolVersion.V2
              , compression : cql.Compression.SNAPPY
              , streamsPerConnection : 1
          );

          conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);

          void handleError(e) {
            expect(e.message, equals("A compression codec needs to be registered via registerCodec() for type '${cql.Compression.SNAPPY}'"));
          }

          conn.open()
          .then((_) {
            Future res = conn.execute(new cql.Query("SELECT * from test.type_test"));

            new Timer(new Duration(milliseconds: 10), () {
              server.replayFile(0, "void_result_v2.dump");

              cql.unregisterCodec(cql.Compression.SNAPPY.value);
            });

            return res;
          })
          .catchError(expectAsync(handleError));
        });

        test("Using a compression codec", () {
          server.setReplayList(["void_result_v2.dump"]);

          // Register codec
          cql.registerCodec(cql.Compression.SNAPPY.value, new compress.MockCompressionCodec());

          cql.PoolConfiguration config = new cql.PoolConfiguration(
              protocolVersion : cql.ProtocolVersion.V2
              , compression : cql.Compression.SNAPPY
          );

          conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);

          expect(
              conn.open().then((_) {
                return conn.execute(new cql.Query("SELECT * from test.type_test"));
              }),
              completion((cql.ResultMessage res) {
                return res is cql.VoidResultMessage;
              })
          );
        });
      });

      group("mock-LZ4:", () {

        setUp(() {
          server.setCompression(cql.Compression.LZ4);
        });

        tearDown(() {
          cql.unregisterCodec(cql.Compression.LZ4.value);
          cql.unregisterCodec(cql.Compression.SNAPPY.value);
        });

        test("Using a compression codec", () {
          server.setCompression(cql.Compression.LZ4);
          server.setReplayList(["void_result_v2.dump"]);

          // Register codec
          cql.registerCodec(cql.Compression.LZ4.value, new compress.MockCompressionCodec());

          cql.PoolConfiguration config = new cql.PoolConfiguration(
              protocolVersion : cql.ProtocolVersion.V2
              , compression : cql.Compression.LZ4
          );

          conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);

          expect(
              conn.open().then((_) {
                return conn.execute(new cql.Query("SELECT * from test.type_test"));
              }),
              completion((cql.ResultMessage res) {
                return res is cql.VoidResultMessage;
              })
          );
        });

        test("Codec decode exception handling", () {
          server.setReplayList(["void_result_v2.dump"]);

          // Register a mock codec for the mock server (pretend its LV4) and the SNAPPY codec
          // with throw on decode for our client. When we receive the server response a decode exception
          // will be thrown
          cql.registerCodec(cql.Compression.LZ4.value, new compress.MockCompressionCodec());
          cql.registerCodec(cql.Compression.SNAPPY.value, new compress.MockCompressionCodec(false, true));

          cql.PoolConfiguration config = new cql.PoolConfiguration(
              protocolVersion : cql.ProtocolVersion.V2
              , compression : cql.Compression.SNAPPY
          );

          conn = new cql.Connection("conn-0", SERVER_HOST, SERVER_PORT, config : config);

          void handleError(e) {
            expect(e.message, startsWith("An error occurred while invoking '${cql.Compression.SNAPPY}' codec (decompression)"));
          }

          conn.open()
          .then((_) => conn.execute(new cql.Query("SELECT * from test.type_test")))
          .catchError(expectAsync(handleError));
        });

        test("Missing compression codec exception", () {
          expect(
                  () => new cql.PoolConfiguration(
                  protocolVersion : cql.ProtocolVersion.V2
                  , compression : cql.Compression.SNAPPY
              )
              , throwsA((e) => e is ArgumentError && e.message == "A compression codec needs to be registered via registerCodec() for type 'snappy'")
          );
        });
      });
    });
  });
}
