library dart_cassandra_cql.tests.query;

import "dart:typed_data";
import "dart:io";
import "package:unittest/unittest.dart";

import "../../dart_cassandra_cql.dart" as cql;
import "mocks/mocks.dart" as mock;
import "mocks/custom.dart" as custom;

main() {
  mock.initLogger();

  group("Single query:", () {

    test("invalid binding type exception", () {
      expect(() => new cql.Query("SELECT foo FROM bar WHERE baz=:\$ AND boo =:boo", bindings : 'boo'),
      throwsArgumentError
      );
    });

    test("invalid placeholder exception for named2positionalQuery", () {
      expect(() => new cql.Query("SELECT foo FROM bar WHERE baz=:\$ AND boo =:boo", bindings : ['boo']).positionalQuery,
      throwsArgumentError
      );
    });

    test("missing custom type codec exception", () {
      custom.CustomJson customJson = new custom.CustomJson({
          "foo" : "bar"
      });
      cql.unregisterCodec(customJson.customTypeClass);
      expect(
          () => new cql.Query("INSERT INTO test.custom_type (login, custom) VALUES ('bar', :baz )", bindings : {
              'baz' : customJson
          }).expandedQuery,
          throwsA((e) => e is ArgumentError && e.message == "No custom type codec specified for type with class: ${customJson.customTypeClass}")
      );

    });

    test("named to positional query missing bindings exception", () {
      cql.Query query = new cql.Query("SELECT foo FROM bar WHERE baz=:baz AND boo=:boo AND baz2=:baz");
      expect(query, new isInstanceOf<cql.QueryInterface>());
      String posQuery = query.positionalQuery;
      query.bindings = {
          'baz' : true
      };
      expect(() => query.namedToPositionalBindings,
      throwsA((e) => e is ArgumentError && e.message == "Missing binding for named placeholder 'boo'")
      );
    });
  });

  group("Single query expansion", () {
    //
    test("with empty bindings", () {
      expect(
          new cql.Query("SELECT foo FROM bar WHERE baz='Simple string' LIMIT 1").expandedQuery,
          "SELECT foo FROM bar WHERE baz='Simple string' LIMIT 1"
      );
    });
    //
    group("of positional args:", () {
      test("ASCII/TEXT", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=? LIMIT 1", bindings : [ 'Simple string' ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz='Simple string' LIMIT 1"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ 'Simple string' ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz='Simple string'"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE skip='this?' AND baz=? OR boo=?", bindings : [ r"'Quoted' string", r"Yet \''Another'\' string" ]).expandedQuery,
            r"SELECT foo FROM bar WHERE skip='this?' AND baz='''Quoted'' string' OR boo='Yet \''''Another''\'' string'"
        );
      });
      test("INT/BIGINT/COUNTER", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ 1 ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=1"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ -3812 ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=-3812"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ 9223372036854775807 ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=9223372036854775807"
        );
      });
      test("BLOB", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ new Uint8List.fromList([0xFE, 0xed, 0xfA, 0xCe]) ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=0xfeedface"
        );

        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [
                new Uint8List.fromList([0x8B, 0xAD, 0xF0, 0x0D, 0xF0, 0x0C, 0x0F, 0xFE])
            ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=0x8badf00df00c0ffe"
        );

      });
      test("BOOLEAN", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=? AND boo=?", bindings : [ true, false ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=true AND boo=false"
        );
      });
      test("DECIMAL/FLOAT/DOUBLE", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ 12.31415161718 ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=12.31415161718"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ -12314151617.18 ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=-12314151617.18"
        );
      });
      test("TIMESTAMP", () {
        DateTime now = new DateTime.now();
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ now ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=${now.millisecondsSinceEpoch}"
        );
      });
      test("INET", () {
        InternetAddress ipv4 = new InternetAddress("192.168.169.101");
        InternetAddress ipv6 = new InternetAddress("2607:f0d0:1002:51::4");
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=? AND boo=?", bindings : [ ipv4, ipv6 ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz='${ipv4.address}' AND boo='${ipv6.address}'"
        );
      });
      test("UUID", () {
        cql.Uuid uuid = new cql.Uuid.simple();
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ uuid ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=${uuid.value}"
        );
      });
      test("LIST", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ [3, 2, 1] ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=[3,2,1]"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ [r'lorem', r"Ips'um", null] ]).expandedQuery,
            r"SELECT foo FROM bar WHERE baz=['lorem','Ips''um',null]"
        );
      });
      test("MAP", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ {
                'a' : 'abc', 'b' : "fe'f"
            } ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz={'a': 'abc', 'b': 'fe''f'}"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ {
                'a' : 1, 'b' : 2, 'c' : null
            } ]).expandedQuery,
            r"SELECT foo FROM bar WHERE baz={'a': 1, 'b': 2, 'c': null}"
        );
      });
      test("SET", () {
        Set<String> set1 = new Set<String>();
        set1.add("abc");
        set1.add("fe'f");

        Set<int> set2 = new Set<int>();
        set2.add(1);
        set2.add(2);

        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ set1 ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=['abc','fe''f']"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ set2 ]).expandedQuery,
            r"SELECT foo FROM bar WHERE baz=[1,2]"
        );
      });
      test("TUPLE", () {
        cql.Tuple tuple = new cql.Tuple.fromIterable(['abc', "fe'f"]);

        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ tuple ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz=('abc','fe''f')"
        );
      });
      test("UDT", () {
        Map udt = {
            'a' : 1,
            'b' : "fe'f",
            'c' : [
                {
                    'foo' : 'bar'
                },
                {
                    'foo' : 'baz'
                }
            ]
        };

        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=?", bindings : [ udt ]).expandedQuery,
            "SELECT foo FROM bar WHERE baz={'a': 1, 'b': 'fe''f', 'c': [{'foo': 'bar'},{'foo': 'baz'}]}"
        );
      });
      test("Missing args exception", () {
        expect(() => new cql.Query("SELECT foo FROM bar WHERE baz=? AND boo = ?", bindings : [ 1 ]).expandedQuery
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "Missing argument '1' from bindings list"
            )
        ));
      });
    });
    //
    group("of named args:", () {
      test("ASCII/TEXT", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz LIMIT 1", bindings : {
                'baz':'Simple string'
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz='Simple string' LIMIT 1"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz':'Simple string'
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz='Simple string'"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE skip=':baz' AND baz=:baz OR boo=:boo", bindings : {
                'baz' : r"'Quoted' string", 'boo' : r"Yet \''Another'\' string"
            }).expandedQuery,
            r"SELECT foo FROM bar WHERE skip=':baz' AND baz='''Quoted'' string' OR boo='Yet \''''Another''\'' string'"
        );
      });
      test("INT/BIGINT/COUNTER", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' :1
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=1"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : -3812
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=-3812"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : 9223372036854775807
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=9223372036854775807"
        );
      });
      test("CUSTOM", () {
        custom.CustomJson customJson = new custom.CustomJson({
            "foo" : "bar"
        });

        // Register custom type handler
        cql.registerCodec(customJson.customTypeClass, new custom.CustomJsonCodec());

        expect(
            new cql.Query("INSERT INTO test.custom_type (login, custom) VALUES ('bar', :baz )", bindings : {
                'baz' : customJson
            }).expandedQuery,
            "INSERT INTO test.custom_type (login, custom) VALUES ('bar', '{\"foo\":\"bar\"}' )"
        );

      });
      test("BLOB", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : new Uint8List.fromList([0xFE, 0xed, 0xfA, 0xCe])
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=0xfeedface"
        );

        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                "baz" : new Uint8List.fromList([0x8B, 0xAD, 0xF0, 0x0D, 0xF0, 0x0C, 0x0F, 0xFE])
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=0x8badf00df00c0ffe"
        );

      });
      test("BOOLEAN", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz AND boo=:boo", bindings : {
                'baz' : true, 'boo' : false
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=true AND boo=false"
        );
      });
      test("DECIMAL/FLOAT/DOUBLE", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : 12.31415161718
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=12.31415161718"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : -12314151617.18
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=-12314151617.18"
        );
      });
      test("TIMESTAMP", () {
        DateTime now = new DateTime.now();
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=   :test", bindings : {
                'test' : now
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=   ${now.millisecondsSinceEpoch}"
        );
      });
      test("INET", () {
        InternetAddress ipv4 = new InternetAddress("192.168.169.101");
        InternetAddress ipv6 = new InternetAddress("2607:f0d0:1002:51::4");
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz AND boo=:boo", bindings : {
                'boo': ipv6, 'baz': ipv4
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz='${ipv4.address}' AND boo='${ipv6.address}'"
        );
      });
      test("UUID", () {
        cql.Uuid uuid = new cql.Uuid.simple();
        expect(uuid.value, equals(uuid.toString()));

        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:uuid", bindings : {
                'uuid': uuid
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=${uuid.value}"
        );
      });
      test("LIST", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : [3, 2, 1]
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=[3,2,1]"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : [r'lorem', r"Ips'um", null]
            }).expandedQuery,
            r"SELECT foo FROM bar WHERE baz=['lorem','Ips''um',null]"
        );
      });
      test("MAP", () {
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : {
                    'a' : 'abc', 'b' : "fe'f"
                }
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz={'a': 'abc', 'b': 'fe''f'}"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : {
                    'a' : 1, 'b' : 2, 'c' : null
                }
            }).expandedQuery,
            r"SELECT foo FROM bar WHERE baz={'a': 1, 'b': 2, 'c': null}"
        );
      });
      test("SET", () {
        Set<String> set1 = new Set<String>();
        set1.add("abc");
        set1.add("fe'f");

        Set<int> set2 = new Set<int>();
        set2.add(1);
        set2.add(2);

        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : set1
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=['abc','fe''f']"
        );
        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : set2
            }).expandedQuery,
            r"SELECT foo FROM bar WHERE baz=[1,2]"
        );
      });
      test("TUPLE", () {
        cql.Tuple tuple = new cql.Tuple.fromIterable(['abc', "fe'f"]);

        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : tuple
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz=('abc','fe''f')"
        );
      });
      test("UDT", () {
        Map udt = {
            'a' : 1,
            'b' : "fe'f",
            'c' : [
                {
                    'foo' : 'bar'
                },
                {
                    'foo' : 'baz'
                }
            ]
        };

        expect(
            new cql.Query("SELECT foo FROM bar WHERE baz=:baz", bindings : {
                'baz' : udt
            }).expandedQuery,
            "SELECT foo FROM bar WHERE baz={'a': 1, 'b': 'fe''f', 'c': [{'foo': 'bar'},{'foo': 'baz'}]}"
        );
      });

      group("Exceptions:", () {
        test("Missing args exception", () {
          expect(() => new cql.Query("SELECT foo FROM bar WHERE baz=:baz AND boo =:boo", bindings : {
              'boo' : 1
          }).expandedQuery
          , throwsA(
              predicate(
                      (e) => e is ArgumentError && e.message == "Missing binding for named placeholder 'baz'"
              )
          ));
        });
        test("Invalid placeholder exception", () {
          expect(() => new cql.Query("SELECT foo FROM bar WHERE baz=:\$ AND boo =:boo", bindings : {
              'boo' : 1
          }).expandedQuery
          , throwsA(
              predicate(
                      (e) => e is ArgumentError && e.message == 'Expected named placeholder to begin at offset 30'
              )
          ));
        });
      });
    });
  });

  group("Batch query expansion", () {
    //
    test("mixed bindings", () {
      cql.BatchQuery batch = new cql.BatchQuery()
        ..add(new cql.Query("SELECT foo FROM bar WHERE baz='Simple string' LIMIT 1"))
        ..add(new cql.Query("SELECT foo FROM bar WHERE baz=? LIMIT 1", bindings : [ 'Simple string' ]))
        ..add(new cql.Query("SELECT foo FROM bar WHERE baz=:baz LIMIT 1", bindings : {
          'baz' : 'Simple string'
      }));
      expect(batch, new isInstanceOf<cql.QueryInterface>());
      expect(
          batch.queryList.map((cql.Query q) => q.expandedQuery),
          [
              "SELECT foo FROM bar WHERE baz='Simple string' LIMIT 1"
              , "SELECT foo FROM bar WHERE baz='Simple string' LIMIT 1"
              , "SELECT foo FROM bar WHERE baz='Simple string' LIMIT 1"
          ]
      );
    });
  });

  group("V2, V3 compatibility:", () {
    test("named to positional query", () {
      cql.Query query = new cql.Query("SELECT foo FROM bar WHERE baz=:baz AND boo=:boo AND baz2=:baz");
      expect(
          query.positionalQuery,
          "SELECT foo FROM bar WHERE baz=? AND boo=? AND baz2=?"
      );
    });
    test("positional to positional query", () {
      cql.Query query = new cql.Query("SELECT foo FROM bar WHERE baz=? AND boo=? AND baz2=?");
      expect(
          query.positionalQuery,
          "SELECT foo FROM bar WHERE baz=? AND boo=? AND baz2=?"
      );
    });
    test("named to positional bindings", () {
      cql.Query query = new cql.Query("SELECT foo FROM bar WHERE baz=:baz AND boo=:boo AND baz2=:baz");
      query.positionalQuery;
      query.bindings = {
          'baz' : true
          , 'boo' : false
      };
      expect(
          query.namedToPositionalBindings,
          [true, false, true]
      );
    });

    test("named to positional bindings (original bindings as a list)", () {
      cql.Query query = new cql.Query("SELECT foo FROM bar WHERE baz=:baz AND boo=:boo AND baz2=:baz");
      query.positionalQuery;
      query.bindings = [ true, false, true ];
      expect(
          query.namedToPositionalBindings,
          [true, false, true]
      );
    });
  });
}