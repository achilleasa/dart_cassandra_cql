library dart_cassandra_cql.tests.client;

import "dart:io";
import "dart:async";
import "dart:typed_data";
import "package:unittest/unittest.dart";
import "mocks/mocks.dart" as mock;
import "mocks/custom.dart" as custom;
import "../../dart_cassandra_cql.dart" as cql;

void main() {
  mock.initLogger();

  const String SERVER_HOST = "127.0.0.1";
  const int SERVER_PORT = 32000;
  mock.MockServer server = new mock.MockServer();
  const int SERVER2_PORT = 32001;
  mock.MockServer server2 = new mock.MockServer();
  cql.Connection conn;

  // Delay server2 responses to make sure that clients *always* connect
  // first so our tests (especially the event ones) execute as they should
  server2.responseDelay = new Duration(milliseconds: 10);

  group("Client exceptions:", () {
    test("Empty host list", () {
      expect(() => new cql.Client.fromHostList([]), throwsArgumentError);
      expect(() => new cql.Client.fromHostList(null), throwsArgumentError);
    });
  });

  group("Simple connection pool:", () {

    cql.Client client;

    setUp(() {
      return Future.wait([
          server.listen(SERVER_HOST, SERVER_PORT)
          , server2.listen(SERVER_HOST, SERVER2_PORT)
      ]);
    });

    tearDown(() {
      List cleanupFutures = [
          server.shutdown()
          , server2.shutdown()
      ];

      if (client != null) {
        cleanupFutures.add(client.shutdown(drain : true));
        client = null;
      }

      return Future.wait(cleanupFutures);
    });

    group("misc:", () {
      test("empty hosts exception", () {
        expect(() => new cql.SimpleConnectionPool.fromHostList(
            []
            , new cql.PoolConfiguration()
        ), throwsA((e) => e is ArgumentError && e.message == "Host list cannot be empty"));
      });

      test("no connection pool exception", () {
        expect(() => new cql.SimpleConnectionPool.fromHostList(
            ['foo:123']
            , null
        ), throwsA((e) => e is ArgumentError && e.message == "A valid pool configuration is required"));
      });

      test("Client with default pool conf", () {
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]);
        expect(client.connectionPool.poolConfig.connectionsPerHost, equals(1));
      });

      test("Fail to connect to any pool host", () {
        client = new cql.Client.fromHostList(
            [ "${SERVER_HOST}:${SERVER_PORT + 3}"]
            , poolConfig : new cql.PoolConfiguration(
                reconnectWaitTime : new Duration(milliseconds : 1)
                , maxConnectionAttempts : 5
            ));

        void handleError(e) {
          expect(e, new isInstanceOf<cql.NoHealthyConnectionsException>());
        }

        client.connectionPool.connect().catchError(expectAsync(handleError));
      });

      test("Connection to at least one node in the pool", () {
        client = new cql.Client.fromHostList(
            [
                "${SERVER_HOST}:${SERVER_PORT}",
                "${SERVER_HOST}:${SERVER_PORT + 3000}"
            ]
            , poolConfig : new cql.PoolConfiguration(
                reconnectWaitTime : new Duration(milliseconds : 1)
                , maxConnectionAttempts : 5
            ));

        void connected(_) {
        }

        client.connectionPool.connect().then(expectAsync(connected));
      });

    });

    group("authentication:", () {
      group("password authenticator exceptions", () {
        test("empty username", () {
          expect(() => new cql.PasswordAuthenticator("", "foo"), throwsArgumentError);
        });

        test("empty password", () {
          expect(() => new cql.PasswordAuthenticator("foo", ""), throwsArgumentError);
        });

      });

      test("Authentication provider exception", () {
        server.setAuthReplayList(["authenticate_v3.dump"]);

        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(
            autoDiscoverNodes : false
            , protocolVersion : cql.ProtocolVersion.V3
        ));

        void handleError(e) {
          expect(e, new isInstanceOf<cql.AuthenticationException>());
          expect(e.message, equals("Server requested 'org.apache.cassandra.auth.PasswordAuthenticator' authenticator but no authenticator specified"));
        }

        client.connectionPool.connect().catchError(expectAsync(handleError));

      });

      test("Different authenticator exception", () {
        server.setAuthReplayList(["authenticate_v3.dump"]);

        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(
            autoDiscoverNodes : false
            , protocolVersion : cql.ProtocolVersion.V3
            , authenticator : new mock.MockAuthenticator()
        ));

        void handleError(e) {
          expect(e, new isInstanceOf<cql.AuthenticationException>());
          expect(e.message, equals("Server requested 'org.apache.cassandra.auth.PasswordAuthenticator' authenticator but a '${client.connectionPool.poolConfig.authenticator.authenticatorClass}' authenticator was specified instead"));
        }

        client.connectionPool.connect().catchError(expectAsync(handleError));

      });

      test("User/pass mismatch exception", () {
        server.setAuthReplayList([
            "authenticate_v3.dump"
            , "auth_error_v3.dump"
        ]);

        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(
            autoDiscoverNodes : false
            , protocolVersion : cql.ProtocolVersion.V3
            , authenticator : new cql.PasswordAuthenticator("foo", "bar")
        ));

        void handleError(e) {
          expect(e, new isInstanceOf<cql.AuthenticationException>());
          expect(e.message, equals("Username and/or password are incorrect"));
        }

        client.connectionPool.connect().catchError(expectAsync(handleError));

      });

      test("Auth success (multi challenge-response)", () {
        server.setAuthReplayList([
            "authenticate_v3.dump"
            , "auth_challenge_v3.dump"
            , "auth_challenge_v3.dump"
            , "auth_challenge_v3.dump"
            , "auth_success_v3.dump"
        ]);
        server.setReplayList(["select_tuple_v3.dump"]);

        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(
            autoDiscoverNodes : false
            , protocolVersion : cql.ProtocolVersion.V3
            , authenticator : new cql.PasswordAuthenticator("foo", "bar")
        ));

        void handleSuccess(_) {
        }

        client.query(new cql.Query("SELECT * FROM test.test_type")).then(expectAsync(handleSuccess));

      });
    });

    group("execute:", () {

      test("select from invalid collection (V3)", () {
        server.setReplayList(["error_v3.dump"]);
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(
            autoDiscoverNodes : false
            , protocolVersion : cql.ProtocolVersion.V3
        ));

        void handleError(e) {
          expect(e, new isInstanceOf<cql.CassandraException>());
          expect(e.message, equals("unconfigured columnfamily foo"));
        }

        client.execute(new cql.Query("SELECT * from test.foo", consistency : cql.Consistency.LOCAL_ONE))
        .catchError(expectAsync(handleError));
      });

      test("set keyspace", () {
        server.setReplayList(["set_keyspace_v2.dump"]);

        cql.ConnectionPool pool = new cql.SimpleConnectionPool.fromHostList([
            "${SERVER_HOST}:${SERVER_PORT}"
        ], new cql.PoolConfiguration(autoDiscoverNodes : false));

        client = new cql.Client.withPool(pool);

        void handleResult(cql.ResultMessage message) {
          expect(message, new isInstanceOf<cql.SetKeyspaceResultMessage>());
          expect((message as cql.SetKeyspaceResultMessage).keyspace, equals("test"));
        }

        client.execute(new cql.Query("USE test")).then(expectAsync(handleResult));
      });

      test("query and process raw RowsResultMessage", () {
        server.setReplayList(["select_v2.dump"]);
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
        );
        expect(
            client.execute(new cql.Query("SELECT * from test.type_test")),
            completion((cql.ResultMessage message) {
              expect(message, new isInstanceOf<cql.RowsResultMessage>());
              cql.RowsResultMessage res = message as cql.RowsResultMessage;
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

      test("alter statement and process raw RowsResultMessage", () {
        server.setReplayList(["schema_change_result_v2.dump"]);
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
        );
        expect(
            client.execute(new cql.Query("ALTER TABLE test.type_test ADD new_field int")),
            completion((cql.ResultMessage message) {
              expect(message, new isInstanceOf<cql.SchemaChangeResultMessage>());
              cql.SchemaChangeResultMessage res = message as cql.SchemaChangeResultMessage;
              expect(res.keyspace, equals("test"));
              expect(res.table, equals("type_test"));
              expect(res.change, equals("UPDATED"));
              return true;
            })
        );
      });
    });

    group("query:", () {

      group("SELECT:", () {

        group("custom types:", () {

          test("without custom type handler", () {
            cql.unregisterCodec('com.achilleasa.cassandra.cqltypes.Json');
            server.setReplayList(["select_custom_type_v2.dump"]);
            client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
            , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
            );

            void onResult(Iterable<Map<String, Object>> rows) {
              expect(rows.length, equals(1));

              Map<String, Object> row = rows.first;
              expect(row.length, equals(2));
              expect(row["login"], equals("test"));
              expect(row["custom"], new isInstanceOf<Uint8List>());
            }

            client.query(new cql.Query("SELECT * from test.custom_types"))
            .then(expectAsync(onResult));
          });

          test("with custom type handler", () {
            // Register custom type handler
            cql.registerCodec('com.achilleasa.cassandra.cqltypes.Json', new custom.CustomJsonCodec());

            server.setReplayList(["select_custom_type_v2.dump"]);
            client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
            , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
            );

            void onResult(Iterable<Map<String, Object>> rows) {
              expect(rows.length, equals(1));

              Map<String, Object> row = rows.first;
              expect(row.length, equals(2));
              expect(row["login"], equals("test"));
              expect(row["custom"], new isInstanceOf<custom.CustomJson>());

              custom.CustomJson customJson = (row["custom"] as custom.CustomJson);
              expect(customJson.payload.containsKey("foo"), isTrue);
              expect(customJson.payload["foo"], equals("bar"));
            }

            client.query(new cql.Query("SELECT * from test.custom_types"))
            .then(expectAsync(onResult));
          });
        });

        test("tuple type", () {
          server.setReplayList(["select_tuple_v3.dump"]);
          client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
          , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false, protocolVersion : cql.ProtocolVersion.V3)
          );
          expect(
              client.query(new cql.Query("SELECT * from test.tuple_test")),
              completion((Iterable<Map<String, Object>> rows) {
                expect(rows.length, equals(1));
                Map<String, Object> row = rows.first;
                Map<String, Object> expectedValues = {
                    "the_key" : 1
                    , "the_tuple" : new cql.Tuple.fromIterable([10, "foo", true])
                };
                expectedValues.forEach((String fieldName, Object fieldValue) {
                  expect(row[fieldName], equals(fieldValue));
                });
                return true;
              })
          );
        });

        test("from unknown collection (V2)", () {
          server.setReplayList(["error_v2.dump"]);
          client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
          , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
          );

          void handleError(e) {
            expect(e, new isInstanceOf<cql.CassandraException>());
            expect(e.message, equals("unconfigured columnfamily foo"));
            expect(e.toString(), equals('CassandraException: ${e.message}'));
          }

          client.query(new cql.Query("SELECT * from test.foo"))
          .catchError(expectAsync(handleError));
        });

        test("normal", () {
          server.setReplayList(["select_v2.dump"]);
          client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
          , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
          );
          expect(
              client.query(new cql.Query("SELECT * from test.type_test")),
              completion((Iterable<Map<String, Object>> rows) {
                expect(rows.length, equals(1));
                Map<String, Object> row = rows.first;
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
      });

      group("INSERT:", () {

        test("batch insert", () {
          server.setReplayList(["void_result_v2.dump"]);
          client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
          , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
          );

          void handleResult(cql.ResultMessage message) {
            expect(message, new isInstanceOf<cql.VoidResultMessage>());
          }

          String query = "INSERT INTO page_view_counts (url_name, page_name, counter_value) VALUES (?, ?, ?)";
          client.execute(
              new cql.BatchQuery()
                ..add(new cql.Query(query, bindings : [ "http://www.test.com", "front_page", 1]))
                ..add(new cql.Query(query, bindings : [ "http://www.test.com", "login_page", 2]))
                ..add(new cql.Query(query, bindings : [ "http://www.test.com", "main_page", 3]))
          ).then(expectAsync(handleResult));
        });

        test("batch insert with serial consistency (V3)", () {
          server.setReplayList(["void_result_v3.dump"]);
          client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
          , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false, protocolVersion : cql.ProtocolVersion.V3)
          );

          void handleResult(cql.ResultMessage message) {
            expect(message, new isInstanceOf<cql.VoidResultMessage>());
          }

          String query = "INSERT INTO page_view_counts (url_name, page_name, counter_value) VALUES (?, ?, ?)";
          client.execute(
              new cql.BatchQuery()
                ..serialConsistency = cql.Consistency.LOCAL_SERIAL
                ..add(new cql.Query(query, bindings : [ "http://www.test.com", "front_page", 1]))
                ..add(new cql.Query(query, bindings : [ "http://www.test.com", "login_page", 2]))
                ..add(new cql.Query(query, bindings : [ "http://www.test.com", "main_page", 3]))
          ).then(expectAsync(handleResult));
        });

      });
    });

    group("stream:", () {

      test("process streamed rows", () {
        server.setReplayList([
            "stream_v2_1of3.dump"
            , "stream_v2_2of3.dump"
            , "stream_v2_3of3.dump"
        ]);
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
        );

        void streamCallback(Map<String, Object> row) {
        }

        client.stream(
            new cql.Query("SELECT * FROM test.page_view_counts")
            , pageSize: 4
        ).listen(expectAsync(streamCallback, count : 10, max : 10));
      });

      test("pause/resume", () {
        server.setReplayList([
            "stream_v2_1of3.dump"
            , "stream_v2_2of3.dump"
            , "stream_v2_3of3.dump"
        ]);
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
        );

        StreamSubscription streamSubscription;
        int rowCount = 0;

        void streamCallback(Map<String, Object> row) {
          rowCount++;
          if (rowCount == 5) {
            streamSubscription.pause();
            new Future.delayed(new Duration(milliseconds : 100), () => streamSubscription.resume());
          }
        }

        streamSubscription = client.stream(
            new cql.Query("SELECT * FROM test.page_view_counts")
            , pageSize: 4
        ).listen(expectAsync(streamCallback, count : 10, max : 10));

      });

      test("close", () {
        server.setReplayList([
            "stream_v2_1of3.dump"
            , "stream_v2_2of3.dump"
            , "stream_v2_3of3.dump"
        ]);
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
        );

        StreamSubscription streamSubscription;
        int rowCount = 0;

        void streamCallback(Map<String, Object> row) {
          rowCount++;
          if (rowCount == 5) {
            streamSubscription.cancel();
          }
        }

        streamSubscription = client.stream(
            new cql.Query("SELECT * FROM test.page_view_counts")
            , pageSize: 4
        ).listen(expectAsync(streamCallback, count : 5, max : 5));
      });

      test("connection lost", () {
        server.setReplayList([
            "stream_v2_1of3.dump"
            , "stream_v2_2of3.dump"
            , "stream_v2_3of3.dump"
        ]);
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
        );

        StreamSubscription subscription;

        bool firstInvocation = true;

        void streamCallback(Map<String, Object> row) {
          if (firstInvocation) {
            firstInvocation = false;
            subscription.pause();
            server.shutdown().then((_) {
              new Future.delayed(new Duration(milliseconds: 10), () => subscription.resume());
            });
          }
        }

        subscription = client.stream(
            new cql.Query("SELECT * FROM test.page_view_counts")
            , pageSize: 4
        ).listen(
            streamCallback
            , onError : expectAsync((e) {
              expect(e, new isInstanceOf<cql.NoHealthyConnectionsException>());
              expect(e.toString(), startsWith("NoHealthyConnectionsException"));
            })
        );
      });

      test("connection lost; fallback to alt connection", () {
        server.setReplayList([
            "stream_v2_1of3.dump"
            , "stream_v2_2of3.dump"
            , "stream_v2_3of3.dump"
        ]);
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false, connectionsPerHost : 2)
        );

        bool firstRun = true;

        void streamCallback(Map<String, Object> row) {
          if (firstRun) {
            firstRun = false;
            server.disconnectClient(0);
          }
        }

        client.stream(
            new cql.Query("SELECT * FROM test.page_view_counts")
            , pageSize: 4
        ).listen(expectAsync(streamCallback, count : 10, max : 10), onError : (e) => print(e));
      });
    });

    group("prepared queries:", () {
      test("prepare and execute query (V2)", () {
        server.setReplayList([
            "prepare_v2.dump"
            , "void_result_v2.dump"
        ]);
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false)
        );

        cql.Query query = new cql.Query("""
INSERT INTO test.type_test (
	ascii_type, bigint_type, decimal_type, bool_type,
	double_type, float_type, inet_type, int_type, list_type, map_type,
	set_type, text_type, timestamp_type, uuid_type, timeuuid_type,
	varchar_type, varint_type, blob_type
) VALUES (
  :ascii_type, :bigint_type, :decimal_type, :bool_type,
	:double_type, :float_type, :inet_type, :int_type, :list_type, :map_type,
	:set_type, :text_type, :timestamp_type, :uuid_type, :timeuuid_type,
	:varchar_type, :varint_type, :blob_type
)""", consistency : cql.Consistency.ONE, prepared : true);

        query.bindings = {
            "ascii_type" : "123"
            , "bigint_type" : 123451234
            , "decimal_type" : 3.14
            , "bool_type" : true
            , "double_type" : 3.14
            , "float_type" : 3.14
            , "inet_type" : new InternetAddress("192.168.169.101")
            , "int_type" : 10
            , "list_type" : [1, 2, 3]
            , "map_type" : {
                1 : "A"
                , 2 : "BC"
            }
            , "set_type" : [1, 2, 3]
            , "text_type" : "Long text"
            , "timestamp_type" : new DateTime.now()
            , "uuid_type" : new cql.Uuid.simple()
            , "timeuuid_type" : new cql.Uuid.timeBased()
            , "varchar_type" : "test 123"
            , "varint_type" : 123456
            , "blob_type" : new Uint8List.fromList([0x8B, 0xAD, 0xF0, 0x00, 0x0D, 0xF0, 0x00, 0x0C, 0x00, 0x0F, 0xF3, 0x0E])
        };

        void done(cql.VoidResultMessage msg) {
        }
        client.execute(query).then(expectAsync(done));
      });

      test("prepare and execute query; fallback to other connection on same host (V2)", () {
        server.setReplayList([
            "prepare_v2.dump"
            , "void_result_v2.dump"
            , "void_result_v2.dump" // 2nd attempt
        ]);
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false, connectionsPerHost: 2)
        );

        cql.Query query = new cql.Query("""
INSERT INTO test.type_test (
	ascii_type, bigint_type, decimal_type, bool_type,
	double_type, float_type, inet_type, int_type, list_type, map_type,
	set_type, text_type, timestamp_type, uuid_type, timeuuid_type,
	varchar_type, varint_type, blob_type
) VALUES (
  :ascii_type, :bigint_type, :decimal_type, :bool_type,
	:double_type, :float_type, :inet_type, :int_type, :list_type, :map_type,
	:set_type, :text_type, :timestamp_type, :uuid_type, :timeuuid_type,
	:varchar_type, :varint_type, :blob_type
)""", consistency : cql.Consistency.ONE, prepared : true);

        query.bindings = {
            "ascii_type" : "123"
            , "bigint_type" : 123451234
            , "decimal_type" : 3.14
            , "bool_type" : true
            , "double_type" : 3.14
            , "float_type" : 3.14
            , "inet_type" : new InternetAddress("192.168.169.101")
            , "int_type" : 10
            , "list_type" : [1, 2, 3]
            , "map_type" : {
                1 : "A"
                , 2 : "BC"
            }
            , "set_type" : [1, 2, 3]
            , "text_type" : "Long text"
            , "timestamp_type" : new DateTime.now()
            , "uuid_type" : new cql.Uuid.simple()
            , "timeuuid_type" : new cql.Uuid.timeBased()
            , "varchar_type" : "test 123"
            , "varint_type" : 123456
            , "blob_type" : new Uint8List.fromList([0x8B, 0xAD, 0xF0, 0x00, 0x0D, 0xF0, 0x00, 0x0C, 0x00, 0x0F, 0xF3, 0x0E])
        };

        Function done = expectAsync((_) {
        });

        client.execute(query)
        .then((_) {
          // Kill 1st connection so we run the next prepared statement attempt
          // on the second connection
          server.disconnectClient(0);

          // 2nd statement should reuse the prepared statement data
          // on the 2nd connection to the same host
          client.execute(query).then(done).catchError(print);
        });
      });

      test("prepare and execute query; prepare on new host after server1 dies (V2)", () {
        server.setReplayList([
            "prepare_v2.dump"
            , "void_result_v2.dump"
        ]);
        server2.setReplayList([
            "prepare_v2.dump"
            , "void_result_v2.dump"
        ]);
        client = new cql.Client.fromHostList([
            "${SERVER_HOST}:${SERVER_PORT}"
            , "${SERVER_HOST}:${SERVER2_PORT}"
        ]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false, connectionsPerHost: 2)
        );

        cql.Query query = new cql.Query("""
INSERT INTO test.type_test (
	ascii_type, bigint_type, decimal_type, bool_type,
	double_type, float_type, inet_type, int_type, list_type, map_type,
	set_type, text_type, timestamp_type, uuid_type, timeuuid_type,
	varchar_type, varint_type, blob_type
) VALUES (
  :ascii_type, :bigint_type, :decimal_type, :bool_type,
	:double_type, :float_type, :inet_type, :int_type, :list_type, :map_type,
	:set_type, :text_type, :timestamp_type, :uuid_type, :timeuuid_type,
	:varchar_type, :varint_type, :blob_type
)""", consistency : cql.Consistency.ONE, prepared : true);

        query.bindings = {
            "ascii_type" : "123"
            , "bigint_type" : 123451234
            , "decimal_type" : 3.14
            , "bool_type" : true
            , "double_type" : 3.14
            , "float_type" : 3.14
            , "inet_type" : new InternetAddress("192.168.169.101")
            , "int_type" : 10
            , "list_type" : [1, 2, 3]
            , "map_type" : {
                1 : "A"
                , 2 : "BC"
            }
            , "set_type" : [1, 2, 3]
            , "text_type" : "Long text"
            , "timestamp_type" : new DateTime.now()
            , "uuid_type" : new cql.Uuid.simple()
            , "timeuuid_type" : new cql.Uuid.timeBased()
            , "varchar_type" : "test 123"
            , "varint_type" : 123456
            , "blob_type" : new Uint8List.fromList([0x8B, 0xAD, 0xF0, 0x00, 0x0D, 0xF0, 0x00, 0x0C, 0x00, 0x0F, 0xF3, 0x0E])
        };

        bool firstResponse = true;

        Future done(cql.VoidResultMessage msg) {
          if (firstResponse) {
            firstResponse = false;
            // Kill 1st server so we prepare the query again on server2
            // on the second connection
            return server.shutdown();
          }

          return new Future.value();
        }

        client.execute(query)
        .then(done)
        // 2nd statement should trigger a prepare on server2
        .then((_) => client.execute(query))
        .then(expectAsync(done));
      });

      test("prepare and execute query; NoHealthyConnections exception after server1 dies (V2)", () {
        server.setReplayList([
            "prepare_v2.dump"
            , "void_result_v2.dump"
        ]);
        client = new cql.Client.fromHostList([
            "${SERVER_HOST}:${SERVER_PORT}"
        ]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false, connectionsPerHost: 2)
        );

        cql.Query query = new cql.Query("""
INSERT INTO test.type_test (
	ascii_type, bigint_type, decimal_type, bool_type,
	double_type, float_type, inet_type, int_type, list_type, map_type,
	set_type, text_type, timestamp_type, uuid_type, timeuuid_type,
	varchar_type, varint_type, blob_type
) VALUES (
  :ascii_type, :bigint_type, :decimal_type, :bool_type,
	:double_type, :float_type, :inet_type, :int_type, :list_type, :map_type,
	:set_type, :text_type, :timestamp_type, :uuid_type, :timeuuid_type,
	:varchar_type, :varint_type, :blob_type
)""", consistency : cql.Consistency.ONE
        , serialConsistency : cql.Consistency.LOCAL_SERIAL
        , prepared : true
        );

        query.bindings = {
            "ascii_type" : "123"
            , "bigint_type" : 123451234
            , "decimal_type" : 3.14
            , "bool_type" : true
            , "double_type" : 3.14
            , "float_type" : 3.14
            , "inet_type" : new InternetAddress("192.168.169.101")
            , "int_type" : 10
            , "list_type" : [1, 2, 3]
            , "map_type" : {
                1 : "A"
                , 2 : "BC"
            }
            , "set_type" : [1, 2, 3]
            , "text_type" : "Long text"
            , "timestamp_type" : new DateTime.now()
            , "uuid_type" : new cql.Uuid.simple()
            , "timeuuid_type" : new cql.Uuid.timeBased()
            , "varchar_type" : "test 123"
            , "varint_type" : 123456
            , "blob_type" : new Uint8List.fromList([0x8B, 0xAD, 0xF0, 0x00, 0x0D, 0xF0, 0x00, 0x0C, 0x00, 0x0F, 0xF3, 0x0E])
        };

        Function fail = expectAsync((e) {
          expect(e, new isInstanceOf<cql.NoHealthyConnectionsException>());
        }, count: 1);

        client.execute(query)
        .then((_) => server.shutdown())
        .then((_) => client.execute(query))
        .catchError(fail);
      });

      test("prepare and execute query (V3)", () {
        server.setReplayList([
            "prepare_v3.dump"
            , "void_result_v3.dump"
        ]);
        client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
        , poolConfig : new cql.PoolConfiguration(autoDiscoverNodes : false, protocolVersion : cql.ProtocolVersion.V3)
        );

        cql.Query query = new cql.Query("""
INSERT INTO test.type_test (
	ascii_type, bigint_type, decimal_type, bool_type,
	double_type, float_type, inet_type, int_type, list_type, map_type,
	set_type, text_type, timestamp_type, uuid_type, timeuuid_type,
	varchar_type, varint_type, blob_type
) VALUES (
  :ascii_type, :bigint_type, :decimal_type, :bool_type,
	:double_type, :float_type, :inet_type, :int_type, :list_type, :map_type,
	:set_type, :text_type, :timestamp_type, :uuid_type, :timeuuid_type,
	:varchar_type, :varint_type, :blob_type
)""", consistency : cql.Consistency.ONE, prepared : true);

        query.bindings = {
            "ascii_type" : "123"
            , "bigint_type" : 123451234
            , "decimal_type" : 3.14
            , "bool_type" : true
            , "double_type" : 3.14
            , "float_type" : 3.14
            , "inet_type" : new InternetAddress("192.168.169.101")
            , "int_type" : 10
            , "list_type" : [1, 2, 3]
            , "map_type" : {
                1 : "A"
                , 2 : "BC"
            }
            , "set_type" : [1, 2, 3]
            , "text_type" : "Long text"
            , "timestamp_type" : new DateTime.now()
            , "uuid_type" : new cql.Uuid.simple()
            , "timeuuid_type" : new cql.Uuid.timeBased()
            , "varchar_type" : "test 123"
            , "varint_type" : 123456
            , "blob_type" : new Uint8List.fromList([0x8B, 0xAD, 0xF0, 0x00, 0x0D, 0xF0, 0x00, 0x0C, 0x00, 0x0F, 0xF3, 0x0E])
        };

        void done(cql.VoidResultMessage msg) {
        }
        client.execute(query).then(expectAsync(done));
      });
    });

    group("server events:", () {
      group("STATUS_CHANGE (V2):", () {
        test("server2 host up event while server1 suddenly dies; automatically connect to server2", () {
          server2.setReplayList([
              "set_keyspace_v2.dump"
          ]);

          cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
              autoDiscoverNodes : true
              , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
          );
          client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER_PORT}"]
          , poolConfig : poolConfig
          );

          void handleResult(cql.ResultMessage message) {
            expect(message, new isInstanceOf<cql.SetKeyspaceResultMessage>());
            expect((message as cql.SetKeyspaceResultMessage).keyspace, equals("test"));
          }

          client.connectionPool
          .connect()
          // Wait for event registration message to be received and then reply the event message
          .then((_) => new Future.delayed(new Duration(milliseconds:100), () => server.replayFile(0, "event_status_up_v2.dump")))
          .then((_) => new Future.delayed(new Duration(milliseconds:100), () => server.shutdown()))
          // Wait for the client to connect to discovered node and try executing a query
          .then((_) => new Future.delayed(new Duration(milliseconds : 200), () => client.execute(new cql.Query("USE test"))))
          .then(expectAsync(handleResult));
        });

        test("server2 host down event; pending queries to server2 should automatically fail", () {
          server2.setReplayList([
              // Intentionally empty so the client gets stuck waiting for the server to reply
          ]);

          cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
              autoDiscoverNodes : true
              , reconnectWaitTime : new Duration(milliseconds : 1) // Keep reconnect time low for our test
          );
          client = new cql.Client.fromHostList([ "${SERVER_HOST}:${SERVER2_PORT}"]
          , poolConfig : poolConfig
          );

          void handleError(e) {
            expect(e, new isInstanceOf<cql.NoHealthyConnectionsException>());
          }

          client.connectionPool
          .connect()
          // Wait for the event registration ready event to arrive and then shut server 2 down
          .then((_) => new Future.delayed(new Duration(milliseconds: 20), () => server2.replayFile(0, "event_status_down_v2.dump")))
          // Wait for the node down message to be processed and attempt a query that should fail
          .then((_) => new Future.delayed(new Duration(milliseconds: 100), () => client.execute(new cql.Query("USE test"))))
          .catchError(expectAsync(handleError));
        });

        test("server2 host down then host up event", () {
          server2.setReplayList([
              "set_keyspace_v2.dump"
          ]);

          cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
              autoDiscoverNodes : true
              , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
          );
          client = new cql.Client.fromHostList(
              [
                  "${SERVER_HOST}:${SERVER_PORT}"
                  , "${SERVER_HOST}:${SERVER2_PORT}"
              ]
              , poolConfig : poolConfig
          );

          void handleResult(cql.ResultMessage message) {
            expect(message, new isInstanceOf<cql.SetKeyspaceResultMessage>());
            expect((message as cql.SetKeyspaceResultMessage).keyspace, equals("test"));
          }

          client.connectionPool
          .connect()
          .then((_) {
            // Wait for event registration message to be received and then reply the event message
            new Timer(new Duration(milliseconds: 100), () => server.replayFile(0, "event_status_down_v2.dump"));
            new Timer(new Duration(milliseconds: 200), () => server.replayFile(0, "event_status_up_v2.dump"));
            return new Future.delayed(new Duration(milliseconds:300), () => server.shutdown());
          })
          .then((_) => client.execute(new cql.Query("USE test")))
          .then(expectAsync(handleResult));
        });

      });

      group("TOPOLOGY CHANGE (V3):", () {
        test("server2 leaves cluster then re-joins", () {
          server2.setReplayList([
              "set_keyspace_v2.dump"
          ]);

          cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
              autoDiscoverNodes : true
              , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
              , protocolVersion : cql.ProtocolVersion.V3
          );
          client = new cql.Client.fromHostList(
              [
                  "${SERVER_HOST}:${SERVER_PORT}"
                  , "${SERVER_HOST}:${SERVER2_PORT}"
              ]
              , poolConfig : poolConfig
          );

          void handleResult(cql.ResultMessage message) {
            expect(message, new isInstanceOf<cql.SetKeyspaceResultMessage>());
            expect((message as cql.SetKeyspaceResultMessage).keyspace, equals("test"));
          }

          client.connectionPool
          .connect()
          .then((_) {
            // Wait for event registration message to be received and then reply the event message
            new Timer(new Duration(milliseconds: 100), () => server.replayFile(0, "event_removed_node_v3.dump"));
            new Timer(new Duration(milliseconds: 200), () => server.replayFile(0, "event_new_node_v3.dump"));
            return new Future.delayed(new Duration(milliseconds:300), () => server.shutdown());
          })
          .then((_) => client.execute(new cql.Query("USE test")))
          .then(expectAsync(handleResult));
        });
      });

      group("SCHEMA CHANGE:", () {
        group("V2:", () {
          group("KEYSPACE:", () {

            test("created", () {

              cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                  autoDiscoverNodes : true
                  , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                  , protocolVersion : cql.ProtocolVersion.V2
              );
              client = new cql.Client.fromHostList(
                  [
                      "${SERVER_HOST}:${SERVER_PORT}"
                  ]
                  , poolConfig : poolConfig
              );

              void handleMessage(cql.EventMessage message) {
                expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
                expect(message.subType, equals(cql.EventType.SCHEMA_CREATED));
                expect(message.keyspace, equals("test"));
                expect(message.changedTable, isNull);
                expect(message.changedType, isNull);
                expect(message.address, isNull);
                expect(message.port, isNull);
              }

              client.connectionPool.listenForServerEvents([
                  cql.EventRegistrationType.SCHEMA_CHANGE
              ]).listen(expectAsync(handleMessage));

              client.connectionPool
              .connect()
              .then((_) {
                // Wait for event registration message to be received and then reply the event message
                return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_created_keyspace_v2.dump"));
              });
            });

            test("dropped", () {

              cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                  autoDiscoverNodes : true
                  , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                  , protocolVersion : cql.ProtocolVersion.V2
              );
              client = new cql.Client.fromHostList(
                  [
                      "${SERVER_HOST}:${SERVER_PORT}"
                  ]
                  , poolConfig : poolConfig
              );

              void handleMessage(cql.EventMessage message) {
                expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
                expect(message.subType, equals(cql.EventType.SCHEMA_DROPPED));
                expect(message.keyspace, equals("test"));
                expect(message.changedTable, isNull);
                expect(message.changedType, isNull);
                expect(message.address, isNull);
                expect(message.port, isNull);
              }

              client.connectionPool.listenForServerEvents([
                  cql.EventRegistrationType.SCHEMA_CHANGE
              ]).listen(expectAsync(handleMessage));

              client.connectionPool
              .connect()
              .then((_) {
                // Wait for event registration message to be received and then reply the event message
                return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_dropped_keyspace_v2.dump"));
              });
            });
          });

          group("TABLE:", () {

            test("created", () {

              cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                  autoDiscoverNodes : true
                  , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                  , protocolVersion : cql.ProtocolVersion.V2
              );
              client = new cql.Client.fromHostList(
                  [
                      "${SERVER_HOST}:${SERVER_PORT}"
                  ]
                  , poolConfig : poolConfig
              );

              void handleMessage(cql.EventMessage message) {
                expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
                expect(message.subType, equals(cql.EventType.SCHEMA_CREATED));
                expect(message.keyspace, equals("test"));
                expect(message.changedTable, "type_test");
                expect(message.changedType, isNull);
                expect(message.address, isNull);
                expect(message.port, isNull);
              }

              client.connectionPool.listenForServerEvents([
                  cql.EventRegistrationType.SCHEMA_CHANGE
              ]).listen(expectAsync(handleMessage));

              client.connectionPool
              .connect()
              .then((_) {
                // Wait for event registration message to be received and then reply the event message
                return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_created_table_v2.dump"));
              });
            });

            test("dropped", () {

              cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                  autoDiscoverNodes : true
                  , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                  , protocolVersion : cql.ProtocolVersion.V3
              );
              client = new cql.Client.fromHostList(
                  [
                      "${SERVER_HOST}:${SERVER_PORT}"
                  ]
                  , poolConfig : poolConfig
              );

              void handleMessage(cql.EventMessage message) {
                expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
                expect(message.subType, equals(cql.EventType.SCHEMA_DROPPED));
                expect(message.keyspace, equals("test"));
                expect(message.changedTable, "type_test");
                expect(message.changedType, isNull);
                expect(message.address, isNull);
                expect(message.port, isNull);
              }

              client.connectionPool.listenForServerEvents([
                  cql.EventRegistrationType.SCHEMA_CHANGE
              ]).listen(expectAsync(handleMessage));

              client.connectionPool
              .connect()
              .then((_) {
                // Wait for event registration message to be received and then reply the event message
                return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_dropped_table_v2.dump"));
              });
            });

            test("updated", () {

              cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                  autoDiscoverNodes : true
                  , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                  , protocolVersion : cql.ProtocolVersion.V2
              );
              client = new cql.Client.fromHostList(
                  [
                      "${SERVER_HOST}:${SERVER_PORT}"
                  ]
                  , poolConfig : poolConfig
              );

              void handleMessage(cql.EventMessage message) {
                expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
                expect(message.subType, equals(cql.EventType.SCHEMA_UPDATED));
                expect(message.keyspace, equals("test"));
                expect(message.changedTable, "type_test");
                expect(message.changedType, isNull);
                expect(message.address, isNull);
                expect(message.port, isNull);
              }

              client.connectionPool.listenForServerEvents([
                  cql.EventRegistrationType.SCHEMA_CHANGE
              ]).listen(expectAsync(handleMessage));

              client.connectionPool
              .connect()
              .then((_) {
                // Wait for event registration message to be received and then reply the event message
                return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_updated_table_v2.dump"));
              });
            });
          });
        });
      });

      group("V3:", () {
        group("KEYSPACE:", () {

          test("created", () {

            cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                autoDiscoverNodes : true
                , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                , protocolVersion : cql.ProtocolVersion.V3
            );
            client = new cql.Client.fromHostList(
                [
                    "${SERVER_HOST}:${SERVER_PORT}"
                ]
                , poolConfig : poolConfig
            );

            void handleMessage(cql.EventMessage message) {
              expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
              expect(message.subType, equals(cql.EventType.SCHEMA_CREATED));
              expect(message.keyspace, equals("test"));
              expect(message.changedTable, isNull);
              expect(message.changedType, isNull);
              expect(message.address, isNull);
              expect(message.port, isNull);
            }

            client.connectionPool.listenForServerEvents([
                cql.EventRegistrationType.SCHEMA_CHANGE
            ]).listen(expectAsync(handleMessage));

            client.connectionPool
            .connect()
            .then((_) {
              // Wait for event registration message to be received and then reply the event message
              return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_created_keyspace_v3.dump"));
            });
          });

          test("dropped", () {

            cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                autoDiscoverNodes : true
                , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                , protocolVersion : cql.ProtocolVersion.V3
            );
            client = new cql.Client.fromHostList(
                [
                    "${SERVER_HOST}:${SERVER_PORT}"
                ]
                , poolConfig : poolConfig
            );

            void handleMessage(cql.EventMessage message) {
              expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
              expect(message.subType, equals(cql.EventType.SCHEMA_DROPPED));
              expect(message.keyspace, equals("test"));
              expect(message.changedTable, isNull);
              expect(message.changedType, isNull);
              expect(message.address, isNull);
              expect(message.port, isNull);
            }

            client.connectionPool.listenForServerEvents([
                cql.EventRegistrationType.SCHEMA_CHANGE
            ]).listen(expectAsync(handleMessage));

            client.connectionPool
            .connect()
            .then((_) {
              // Wait for event registration message to be received and then reply the event message
              return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_dropped_keyspace_v3.dump"));
            });
          });
        });

        group("TABLE:", () {

          test("created", () {

            cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                autoDiscoverNodes : true
                , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                , protocolVersion : cql.ProtocolVersion.V3
            );
            client = new cql.Client.fromHostList(
                [
                    "${SERVER_HOST}:${SERVER_PORT}"
                ]
                , poolConfig : poolConfig
            );

            void handleMessage(cql.EventMessage message) {
              expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
              expect(message.subType, equals(cql.EventType.SCHEMA_CREATED));
              expect(message.keyspace, equals("test"));
              expect(message.changedTable, "type_test");
              expect(message.changedType, isNull);
              expect(message.address, isNull);
              expect(message.port, isNull);
            }

            client.connectionPool.listenForServerEvents([
                cql.EventRegistrationType.SCHEMA_CHANGE
            ]).listen(expectAsync(handleMessage));

            client.connectionPool
            .connect()
            .then((_) {
              // Wait for event registration message to be received and then reply the event message
              return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_created_table_v3.dump"));
            });
          });

          test("dropped", () {

            cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                autoDiscoverNodes : true
                , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                , protocolVersion : cql.ProtocolVersion.V3
            );
            client = new cql.Client.fromHostList(
                [
                    "${SERVER_HOST}:${SERVER_PORT}"
                ]
                , poolConfig : poolConfig
            );

            void handleMessage(cql.EventMessage message) {
              expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
              expect(message.subType, equals(cql.EventType.SCHEMA_DROPPED));
              expect(message.keyspace, equals("test"));
              expect(message.changedTable, "type_test");
              expect(message.changedType, isNull);
              expect(message.address, isNull);
              expect(message.port, isNull);
            }

            client.connectionPool.listenForServerEvents([
                cql.EventRegistrationType.SCHEMA_CHANGE
            ]).listen(expectAsync(handleMessage));

            client.connectionPool
            .connect()
            .then((_) {
              // Wait for event registration message to be received and then reply the event message
              return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_dropped_table_v3.dump"));
            });
          });

          test("updated", () {

            cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                autoDiscoverNodes : true
                , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                , protocolVersion : cql.ProtocolVersion.V3
            );
            client = new cql.Client.fromHostList(
                [
                    "${SERVER_HOST}:${SERVER_PORT}"
                ]
                , poolConfig : poolConfig
            );

            void handleMessage(cql.EventMessage message) {
              expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
              expect(message.subType, equals(cql.EventType.SCHEMA_UPDATED));
              expect(message.keyspace, equals("test"));
              expect(message.changedTable, "type_test");
              expect(message.changedType, isNull);
              expect(message.address, isNull);
              expect(message.port, isNull);
            }

            client.connectionPool.listenForServerEvents([
                cql.EventRegistrationType.SCHEMA_CHANGE
            ]).listen(expectAsync(handleMessage));

            client.connectionPool
            .connect()
            .then((_) {
              // Wait for event registration message to be received and then reply the event message
              return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_updated_table_v3.dump"));
            });
          });
        });

        group("TYPE:", () {

          test("created", () {

            cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                autoDiscoverNodes : true
                , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                , protocolVersion : cql.ProtocolVersion.V3
            );
            client = new cql.Client.fromHostList(
                [
                    "${SERVER_HOST}:${SERVER_PORT}"
                ]
                , poolConfig : poolConfig
            );

            void handleMessage(cql.EventMessage message) {
              expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
              expect(message.subType, equals(cql.EventType.SCHEMA_CREATED));
              expect(message.keyspace, equals("test"));
              expect(message.changedTable, isNull);
              expect(message.changedType, "phone");
              expect(message.address, isNull);
              expect(message.port, isNull);
            }

            client.connectionPool.listenForServerEvents([
                cql.EventRegistrationType.SCHEMA_CHANGE
            ]).listen(expectAsync(handleMessage));

            client.connectionPool
            .connect()
            .then((_) {
              // Wait for event registration message to be received and then reply the event message
              return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_created_type_v3.dump"));
            });
          });

          test("dropped", () {

            cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                autoDiscoverNodes : true
                , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                , protocolVersion : cql.ProtocolVersion.V3
            );
            client = new cql.Client.fromHostList(
                [
                    "${SERVER_HOST}:${SERVER_PORT}"
                ]
                , poolConfig : poolConfig
            );

            void handleMessage(cql.EventMessage message) {
              expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
              expect(message.subType, equals(cql.EventType.SCHEMA_DROPPED));
              expect(message.keyspace, equals("test"));
              expect(message.changedTable, isNull);
              expect(message.changedType, "phone");
              expect(message.address, isNull);
              expect(message.port, isNull);
            }

            client.connectionPool.listenForServerEvents([
                cql.EventRegistrationType.SCHEMA_CHANGE
            ]).listen(expectAsync(handleMessage));

            client.connectionPool
            .connect()
            .then((_) {
              // Wait for event registration message to be received and then reply the event message
              return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_dropped_type_v3.dump"));
            });
          });

          test("updated", () {

            cql.PoolConfiguration poolConfig = new cql.PoolConfiguration(
                autoDiscoverNodes : true
                , reconnectWaitTime : new Duration(milliseconds : 0) // Keep reconnect time low for our test
                , protocolVersion : cql.ProtocolVersion.V3
            );
            client = new cql.Client.fromHostList(
                [
                    "${SERVER_HOST}:${SERVER_PORT}"
                ]
                , poolConfig : poolConfig
            );

            void handleMessage(cql.EventMessage message) {
              expect(message.type, equals(cql.EventRegistrationType.SCHEMA_CHANGE));
              expect(message.subType, equals(cql.EventType.SCHEMA_UPDATED));
              expect(message.keyspace, equals("test"));
              expect(message.changedTable, isNull);
              expect(message.changedType, "phone");
              expect(message.address, isNull);
              expect(message.port, isNull);
            }

            client.connectionPool.listenForServerEvents([
                cql.EventRegistrationType.SCHEMA_CHANGE
            ]).listen(expectAsync(handleMessage));

            client.connectionPool
            .connect()
            .then((_) {
              // Wait for event registration message to be received and then reply the event message
              return new Future.delayed(new Duration(milliseconds: 100), () => server.replayFile(0, "event_schema_updated_type_v3.dump"));
            });
          });
        });

      });
    });
  });
}
