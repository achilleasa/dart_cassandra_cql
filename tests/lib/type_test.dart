library dart_cassandra_cql.tests.type_guess;

import "dart:typed_data";
import "dart:io";
import "package:unittest/unittest.dart";

import "../../lib/types.dart";
import 'mocks/mocks.dart' as mock;
import "mocks/custom.dart" as custom;

main() {
  mock.initLogger();

  group("Collection type:", () {
    test("isCollection(LIST)", () {
      expect(DataType.SET.isCollection, isTrue);
    });
    test("isCollection(SET)", () {
      expect(DataType.LIST.isCollection, isTrue);
    });
    test("isCollection(MAP)", () {
      expect(DataType.MAP.isCollection, isTrue);
    });
    test("isCollection(TUPLE)", () {
      expect(DataType.TUPLE.isCollection, isFalse);
    });
  });

  group("TypeSpec.toString():", () {
    test("ASCII", () {
      TypeSpec ts = new TypeSpec(DataType.ASCII);
      expect(ts.toString(), equals("ASCII"));
    });

    test("CUSTOM", () {
      custom.CustomJson customJson = new custom.CustomJson({});
      TypeSpec ts = new TypeSpec(DataType.CUSTOM)
      ..customTypeClass = customJson.customTypeClass;
      expect(ts.toString(), equals("CustomType<${customJson.customTypeClass}>"));
    });

    test("LIST", () {
      TypeSpec ts = new TypeSpec(
          DataType.LIST
          , valueSubType : new TypeSpec(DataType.INET)
      );
      expect(ts.toString(), equals("List<INET>"));
    });

    test("SET", () {
      TypeSpec ts = new TypeSpec(
          DataType.SET
          , valueSubType : new TypeSpec(DataType.TIMESTAMP)
      );
      expect(ts.toString(), equals("Set<TIMESTAMP>"));
    });

    test("MAP", () {
      TypeSpec ts = new TypeSpec(
          DataType.MAP
          , keySubType : new TypeSpec(DataType.TIMESTAMP)
          , valueSubType : new TypeSpec(DataType.INT)
      );
      expect(ts.toString(), equals("Map<TIMESTAMP, INT>"));
    });

    test("UDT", () {
      TypeSpec ts = new TypeSpec(
          DataType.UDT
      )
        ..keyspace = "test"
        ..udtName = "phone"
        ..udtFields = {
          "tags" : new TypeSpec(
              DataType.LIST
              , valueSubType : new TypeSpec(DataType.ASCII)
          )
      };
      expect(ts.toString(), equals('{test.phone: {tags: List<ASCII>}}'));
    });

    test("TUPLE", () {
      TypeSpec ts = new TypeSpec(
          DataType.TUPLE
      )
        ..tupleFields = [
          new TypeSpec(DataType.INT)
          , new TypeSpec(DataType.ASCII)
          , new TypeSpec(DataType.TIMESTAMP)
      ];
      expect(ts.toString(), equals('([INT, ASCII, TIMESTAMP])'));
    });
  });

  group("Type guess:", () {

    test("BOOL", () {
      expect(
          DataType.guessForValue(true)
          , equals(DataType.BOOLEAN)
      );
      expect(
          DataType.guessForValue(false)
          , equals(DataType.BOOLEAN)
      );
    });

    test("DOUBLE", () {
      expect(
          DataType.guessForValue(3.145)
          , equals(DataType.DOUBLE)
      );
    });

    test("INT", () {
      expect(
          DataType.guessForValue(3)
          , equals(DataType.INT)
      );
    });

    test("BIGINT", () {
      expect(
          DataType.guessForValue(9223372036854775807)
          , equals(DataType.BIGINT)
      );
    });

    test("VARINT", () {
      expect(
          DataType.guessForValue(9223372036854775807000000)
          , equals(DataType.VARINT)
      );
    });

    test("VARCHAR", () {
      expect(
          DataType.guessForValue("test123 123")
          , equals(DataType.VARCHAR)
      );
    });

    test("UUID", () {
      expect(
          DataType.guessForValue(new Uuid.simple())
          , equals(DataType.UUID)
      );

      expect(
          DataType.guessForValue(new Uuid.timeBased())
          , equals(DataType.UUID)
      );

      expect(
          DataType.guessForValue(new Uuid.timeBased().toString())
          , equals(DataType.UUID)
      );
    });

    test("BLOB", () {
      expect(
          DataType.guessForValue(new Uint8List.fromList([0xff]))
          , equals(DataType.BLOB)
      );
    });

    test("TIMESTAMP", () {
      expect(
          DataType.guessForValue(new DateTime.now())
          , equals(DataType.TIMESTAMP)
      );
    });

    test("INET", () {
      expect(
          DataType.guessForValue(new InternetAddress("127.0.0.1"))
          , equals(DataType.INET)
      );
    });

    test("LIST", () {
      expect(
          DataType.guessForValue(["test123 123", 1, 2, 3.14])
          , equals(DataType.LIST)
      );
    });

    test("SET", () {
      expect(
          DataType.guessForValue(new Set.from(["a", "a", "b"]))
          , equals(DataType.SET)
      );
    });

    test("MAP", () {
      expect(
          DataType.guessForValue({
              "foo" : "bar"
          })
          , equals(DataType.MAP)
      );
    });

    test("TUPLE", () {
      expect(
          DataType.guessForValue(new Tuple.fromIterable([1, 2, 3]))
          , equals(DataType.TUPLE)
      );
    });

    test("No guess", () {
      expect(
          DataType.guessForValue(new SocketException("foo"))
          , isNull
      );
    });

  });
}