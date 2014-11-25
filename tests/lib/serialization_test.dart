library dart_cassandra_cql.tests.serialization;

import "dart:typed_data";
import "dart:io";
import "dart:math";
import "package:unittest/unittest.dart";

import "../../lib/stream.dart";
import "../../lib/types.dart";
import "mocks/mocks.dart" as mock;
import 'mocks/custom.dart' as custom;

main() {
  mock.initLogger();

  final custom.CustomJson customJsonInstance = new custom.CustomJson({
  });

  group("Serialization", () {
    TypeEncoder encoder;
    SizeType size;

    group("Exceptions:", () {
      setUp(() {
        encoder = new TypeEncoder(ProtocolVersion.V3);
        size = SizeType.LONG;
      });

      tearDown(() {
        unregisterCodec(customJsonInstance.customTypeClass);
      });

      group("TypeSpec:", () {

        test("Missing key/valueSubTYpe", () {
          expect(() => new TypeSpec(DataType.MAP)
          , throwsA(
              predicate(
                      (e) => e is ArgumentError && e.message == "MAP type should specify TypeSpec instances for both its keys and values"
              )
          ));

          expect(() => new TypeSpec(DataType.MAP, keySubType : new TypeSpec(DataType.ASCII))
          , throwsA(
              predicate(
                      (e) => e is ArgumentError && e.message == "MAP type should specify TypeSpec instances for both its keys and values"
              )
          ));

          expect(() => new TypeSpec(DataType.MAP, valueSubType : new TypeSpec(DataType.ASCII))
          , throwsA(
              predicate(
                      (e) => e is ArgumentError && e.message == "MAP type should specify TypeSpec instances for both its keys and values"
              )
          ));

        });

        test("Missing valueSubType", () {
          expect(() => new TypeSpec(DataType.LIST)
          , throwsA(
              predicate(
                      (e) => e is ArgumentError && e.message == "LIST type should specify a TypeSpec instance for its values"
              )
          ));

          expect(() => new TypeSpec(DataType.SET)
          , throwsA(
              predicate(
                      (e) => e is ArgumentError && e.message == "SET type should specify a TypeSpec instance for its values"
              )
          ));

        });

      });

      test("Not instance of DateTime", () {
        Object input = "foo";
        TypeSpec type = new TypeSpec(DataType.TIMESTAMP);
        expect(() => encoder.writeTypedValue('test', input, typeSpec : type, size : size)
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "Expected value for field 'test' of type TIMESTAMP to be an instance of DateTime"
            )
        ));

      });

      test("Not instance of Uint8List or CustomType", () {
        Object input = "foo";
        TypeSpec type = new TypeSpec(DataType.CUSTOM);
        expect(() => encoder.writeTypedValue('test', input, typeSpec : type, size : size)
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "Expected value for field 'test' of type CUSTOM to be an instance of Uint8List OR an instance of CustomType with a registered type handler"
            )
        ));

        expect(() => encoder.writeTypedValue('test', new Uint16List.fromList([]), typeSpec : type, size : size)
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "Expected value for field 'test' of type CUSTOM to be an instance of Uint8List OR an instance of CustomType with a registered type handler"
            )
        ));

        unregisterCodec(customJsonInstance.customTypeClass);
        expect(() => encoder.writeTypedValue('test', customJsonInstance, typeSpec : type, size : size)
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "No custom type handler codec registered for custom type: ${customJsonInstance.customTypeClass}"
            )
        ));

        type = new TypeSpec(DataType.BLOB);
        expect(() => encoder.writeTypedValue('test', input, typeSpec : type, size : size)
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "Expected value for field 'test' of type BLOB to be an instance of Uint8List"
            )
        ));

      });

      test("Not instance of InternetAddress", () {
        Object input = "foo";
        TypeSpec type = new TypeSpec(DataType.INET);
        expect(() => encoder.writeTypedValue('test', input, typeSpec : type, size : size)
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "Expected value for field 'test' of type INET to be an instance of InternetAddress"
            )
        ));

      });

      test("Not instance of Iterable", () {
        Object input = "foo";
        TypeSpec type = new TypeSpec(DataType.LIST, valueSubType : new TypeSpec(DataType.ASCII));
        expect(() => encoder.writeTypedValue('test', input, typeSpec : type, size : size)
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "Expected value for field 'test' of type LIST to implement Iterable"
            )
        ));
        type = new TypeSpec(DataType.SET, valueSubType : new TypeSpec(DataType.ASCII));
        expect(() => encoder.writeTypedValue('test', input, typeSpec : type, size : size)
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "Expected value for field 'test' of type SET to implement Iterable"
            )
        ));

      });

      test("Not instance of Map", () {
        Object input = "foo";
        TypeSpec type = new TypeSpec(DataType.MAP, keySubType : new TypeSpec(DataType.ASCII), valueSubType : new TypeSpec(DataType.ASCII));
        expect(() => encoder.writeTypedValue('test', input, typeSpec : type, size : size)
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "Expected value for field 'test' of type MAP to implement Map"
            )
        ));

        type = new TypeSpec(DataType.UDT);
        expect(() => encoder.writeTypedValue('test', input, typeSpec : type, size : size)
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "Expected value for field 'test' of type UDT to implement Map"
            )
        ));
      });

      test("Not instance of Tuple", () {
        Object input = ["foo"];
        TypeSpec type = new TypeSpec(DataType.TUPLE);
        expect(() => encoder.writeTypedValue('test', input, typeSpec : type, size : size)
        , throwsA(
            predicate(
                    (e) => e is ArgumentError && e.message == "Expected value for field 'test' of type TUPLE to be an instance of Tuple"
            )
        ));

      });
    });

    group("Internal types:", () {
      setUp(() {
        encoder = new TypeEncoder(ProtocolVersion.V2);
        size = SizeType.SHORT;
      });

      test("Consistency", () {
        Consistency input = Consistency.LOCAL_ONE;
        encoder.writer.addLast(new Uint8List.fromList([ 0x00, input.value ]));
        Object output = mock.createDecoder(encoder).readConsistency();

        expect(output, equals(input));
      });

      test("String list", () {
        List<String> input = [ "a", "foo", "f33dfAce"];
        encoder.writeStringList(input, size);
        Object output = mock.createDecoder(encoder).readStringList(size);
        expect(output, equals(input));
      });

      test("String map", () {
        Map<String, String> input = {
            "foo" : "bar"
            , "baz00ka" : "f33df4ce"
        };
        encoder.writeStringMap(input, size);
        Object output = mock.createDecoder(encoder).readStringMap(size);
        expect(output, equals(input));
      });

      test("String multimap", () {
        Map<String, List<String>> input = {
            "foo" : ["bar", "baz"]
            , "baz00ka" : ["f33df4ce"]
        };
        encoder.writeStringMultiMap(input, size);
        Object output = mock.createDecoder(encoder).readStringMultiMap(size);
        expect(output, equals(input));
      });

    });

    group("(protocol V2):", () {
      setUp(() {
        encoder = new TypeEncoder(ProtocolVersion.V2);
        size = SizeType.SHORT;
      });

      test("UTF-8 STRING", () {
        Object input = "Test 123 AbC !@#ΤΕΣΤ";
        TypeSpec type = new TypeSpec(DataType.TEXT);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      test("ASCII STRING", () {
        Object input = "Test 123 AbC";
        TypeSpec type = new TypeSpec(DataType.ASCII);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      test("UUID", () {
        Object input = new Uuid.simple();
        TypeSpec type = new TypeSpec(DataType.UUID);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      test("TIMEUUID", () {
        Object input = new Uuid.timeBased();
        TypeSpec type = new TypeSpec(DataType.TIMEUUID);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      group("CUSTOM:", () {

        test("without type handler", () {
          Object input = new Uint8List.fromList(new List<int>.generate(10, (int index) => index * 2));
          TypeSpec type = new TypeSpec(DataType.CUSTOM)
            ..customTypeClass = customJsonInstance.customTypeClass;
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("with type handler", () {
          // Register custom type handler
          registerCodec('com.achilleasa.cassandra.cqltypes.Json', new custom.CustomJsonCodec());

          customJsonInstance.payload = {
              "foo" : {
                  "bar" : "baz"
              }
          };

          TypeSpec type = new TypeSpec(DataType.CUSTOM)
            ..customTypeClass = customJsonInstance.customTypeClass;

          encoder.writeTypedValue('test', customJsonInstance, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);
          expect(output, new isInstanceOf<custom.CustomJson>());
          expect((output as custom.CustomJson).payload, equals(customJsonInstance.payload));
        });

      });

      test("BLOB", () {
        Object input = new Uint8List.fromList(new List<int>.generate(10, (int index) => index * 2));
        TypeSpec type = new TypeSpec(DataType.BLOB);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      group("COUNTER", () {

        test("(positive)", () {
          Object input = 9223372036854775807;
          TypeSpec type = new TypeSpec(DataType.COUNTER);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("(negative)", () {
          Object input = -1;
          TypeSpec type = new TypeSpec(DataType.COUNTER);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

      });

      test("TIMESTAMP", () {

        Object input = new DateTime.now();
        TypeSpec type = new TypeSpec(DataType.TIMESTAMP);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      group("BOOLEAN", () {
        test("(true)", () {

          Object input = true;
          TypeSpec type = new TypeSpec(DataType.BOOLEAN);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("(false)", () {

          Object input = false;
          TypeSpec type = new TypeSpec(DataType.BOOLEAN);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

      });

      group("BOOLEAN", () {
        test("(true)", () {

          Object input = true;
          TypeSpec type = new TypeSpec(DataType.BOOLEAN);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("(false)", () {

          Object input = false;
          TypeSpec type = new TypeSpec(DataType.BOOLEAN);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

      });

      group("INET:", () {
        test("(ipv4)", () {
          Object input = new InternetAddress("192.168.169.101");
          TypeSpec type = new TypeSpec(DataType.INET);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("(ipv6)", () {
          Object input = new InternetAddress("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
          TypeSpec type = new TypeSpec(DataType.INET);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });
      });

      group("NUMBERS:", () {
        group("INT", () {
          test("(positive)", () {
            Object input = 2147483647;
            TypeSpec type = new TypeSpec(DataType.INT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            Object input = -21474836;
            TypeSpec type = new TypeSpec(DataType.INT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));
          });

        });

        group("BIGINT", () {

          test("(positive)", () {
            Object input = 9223372036854775807;
            TypeSpec type = new TypeSpec(DataType.BIGINT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            Object input = -922036854775807;
            TypeSpec type = new TypeSpec(DataType.BIGINT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));
          });

        });

        group("FLOAT", () {

          test("(positive)", () {
            Object input = 3.141516;
            TypeSpec type = new TypeSpec(DataType.FLOAT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, 0.000001));
          });

          test("(negative)", () {
            Object input = -3.12345;
            TypeSpec type = new TypeSpec(DataType.FLOAT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, 0.000001));
          });

        });

        group("DOUBLE", () {

          test("(positive)", () {
            Object input = 3.141516;
            TypeSpec type = new TypeSpec(DataType.DOUBLE);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, 0.000001));

          });

          test("(negative)", () {
            Object input = -3.12345;
            TypeSpec type = new TypeSpec(DataType.DOUBLE);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, 0.000001));
          });

        });

        group("DECIMAL [fraction digits = ${DECIMAL_FRACTION_DIGITS}]", () {

          test("(positive)", () {
            Object input = 3.123451234512345;
            TypeSpec type = new TypeSpec(DataType.DECIMAL);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, pow(10, -DECIMAL_FRACTION_DIGITS)));

          });

          test("(negative)", () {
            Object input = -3.123451234512345;
            TypeSpec type = new TypeSpec(DataType.DECIMAL);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, pow(10, -DECIMAL_FRACTION_DIGITS)));
          });

        });

        group("VARINT", () {

          test("(positive)", () {
            Object input = 12345678901234567890123;
            TypeSpec type = new TypeSpec(DataType.VARINT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));

          });

          test("(negative)", () {
            Object input = -987677654324167384628746291873912873;
            TypeSpec type = new TypeSpec(DataType.VARINT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));
          });

        });
      });

      group("COLLECTIONS:", () {
        test("SET", () {
          Object input = new Set.from([ -2, -1, 0, 1, 2 ]);
          TypeSpec type = new TypeSpec(DataType.SET, valueSubType : new TypeSpec(DataType.INT));
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("LIST", () {
          Object input = [ new DateTime.now(), new DateTime.now() ];
          TypeSpec type = new TypeSpec(DataType.LIST, valueSubType : new TypeSpec(DataType.TIMESTAMP));
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("MAP", () {
          Object input = {
              "foo" : new DateTime.now(),
              "bar" : new DateTime.now()
          };
          TypeSpec type = new TypeSpec(
              DataType.MAP
              , keySubType : new TypeSpec(DataType.TEXT)
              , valueSubType : new TypeSpec(DataType.TIMESTAMP)
          );
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });
      });

    });

    group("(protocol V3):", () {
      setUp(() {
        encoder = new TypeEncoder(ProtocolVersion.V3);
        size = SizeType.LONG;
      });
      test("UTF-8 STRING", () {
        Object input = "Test 123 AbC !@#ΤΕΣΤ";
        TypeSpec type = new TypeSpec(DataType.TEXT);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      test("ASCII STRING", () {
        Object input = "Test 123 AbC";
        TypeSpec type = new TypeSpec(DataType.ASCII);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      test("UUID", () {
        Object input = new Uuid.simple();
        TypeSpec type = new TypeSpec(DataType.UUID);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      test("TIMEUUID", () {
        Object input = new Uuid.timeBased();
        TypeSpec type = new TypeSpec(DataType.TIMEUUID);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      test("CUSTOM", () {
        Object input = new Uint8List.fromList(new List<int>.generate(10, (int index) => index * 2));
        TypeSpec type = new TypeSpec(DataType.CUSTOM);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      test("BLOB", () {
        Object input = new Uint8List.fromList(new List<int>.generate(10, (int index) => index * 2));
        TypeSpec type = new TypeSpec(DataType.BLOB);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      group("COUNTER", () {

        test("(positive)", () {
          Object input = 9223372036854775807;
          TypeSpec type = new TypeSpec(DataType.COUNTER);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("(negative)", () {
          Object input = -1;
          TypeSpec type = new TypeSpec(DataType.COUNTER);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

      });

      test("TIMESTAMP", () {

        Object input = new DateTime.now();
        TypeSpec type = new TypeSpec(DataType.TIMESTAMP);
        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      group("BOOLEAN", () {
        test("(true)", () {

          Object input = true;
          TypeSpec type = new TypeSpec(DataType.BOOLEAN);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("(false)", () {

          Object input = false;
          TypeSpec type = new TypeSpec(DataType.BOOLEAN);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

      });

      group("BOOLEAN", () {
        test("(true)", () {

          Object input = true;
          TypeSpec type = new TypeSpec(DataType.BOOLEAN);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("(false)", () {

          Object input = false;
          TypeSpec type = new TypeSpec(DataType.BOOLEAN);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

      });

      group("INET:", () {
        test("(ipv4)", () {
          Object input = new InternetAddress("192.168.169.101");
          TypeSpec type = new TypeSpec(DataType.INET);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("(ipv6)", () {
          Object input = new InternetAddress("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
          TypeSpec type = new TypeSpec(DataType.INET);
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });
      });

      group("NUMBERS:", () {
        group("INT", () {
          test("(positive)", () {
            Object input = 2147483647;
            TypeSpec type = new TypeSpec(DataType.INT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            Object input = -21474836;
            TypeSpec type = new TypeSpec(DataType.INT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));
          });

        });

        group("BIGINT", () {

          test("(positive)", () {
            Object input = 9223372036854775807;
            TypeSpec type = new TypeSpec(DataType.BIGINT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            Object input = -922036854775807;
            TypeSpec type = new TypeSpec(DataType.BIGINT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));
          });

        });

        group("FLOAT", () {

          test("(positive)", () {
            Object input = 3.141516;
            TypeSpec type = new TypeSpec(DataType.FLOAT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, 0.000001));
          });

          test("(negative)", () {
            Object input = -3.12345;
            TypeSpec type = new TypeSpec(DataType.FLOAT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, 0.000001));
          });

        });

        group("DOUBLE", () {

          test("(positive)", () {
            Object input = 3.141516;
            TypeSpec type = new TypeSpec(DataType.DOUBLE);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, 0.000001));

          });

          test("(negative)", () {
            Object input = -3.12345;
            TypeSpec type = new TypeSpec(DataType.DOUBLE);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, 0.000001));
          });

        });

        group("DECIMAL [fraction digits = ${DECIMAL_FRACTION_DIGITS}]", () {

          test("(positive)", () {
            Object input = 3.123451234512345;
            TypeSpec type = new TypeSpec(DataType.DECIMAL);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, pow(10, -DECIMAL_FRACTION_DIGITS)));

          });

          test("(negative)", () {
            Object input = -3.123451234512345;
            TypeSpec type = new TypeSpec(DataType.DECIMAL);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, closeTo(input, pow(10, -DECIMAL_FRACTION_DIGITS)));
          });

        });

        group("VARINT", () {

          test("(positive)", () {
            Object input = 12345678901234567890123;
            TypeSpec type = new TypeSpec(DataType.VARINT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));

          });

          test("(negative)", () {
            Object input = -987677654324167384628746291873912873;
            TypeSpec type = new TypeSpec(DataType.VARINT);
            encoder.writeTypedValue('test', input, typeSpec : type, size : size);
            Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

            expect(output, equals(input));
          });

        });
      });

      group("COLLECTIONS:", () {
        test("SET", () {
          Object input = new Set.from([ -2, -1, 0, 1, 2 ]);
          TypeSpec type = new TypeSpec(DataType.SET, valueSubType : new TypeSpec(DataType.INT));
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("LIST", () {
          Object input = [ new DateTime.now(), new DateTime.now() ];
          TypeSpec type = new TypeSpec(DataType.LIST, valueSubType : new TypeSpec(DataType.TIMESTAMP));
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });

        test("MAP", () {
          Object input = {
              "foo" : new DateTime.now(),
              "bar" : new DateTime.now()
          };
          TypeSpec type = new TypeSpec(
              DataType.MAP
              , keySubType : new TypeSpec(DataType.TEXT)
              , valueSubType : new TypeSpec(DataType.TIMESTAMP)
          );
          encoder.writeTypedValue('test', input, typeSpec : type, size : size);
          Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

          expect(output, equals(input));
        });
      });

      test("UDT (nested)", () {
        Object input = {
            "address" : "Elm street"
            , "phones" : [
                {
                    "prefix" : 30
                    , "phone" : "123456789"
                },
                {
                    "prefix" : 1
                    , "phone" : "800180023"
                }
            ]
            , "tags" : {
                "home" : {
                    "when" : new DateTime.now()
                    , "labels" : [ "red", "green", "blue" ]
                }
            }
        };
        TypeSpec intType = new TypeSpec(DataType.INT);
        TypeSpec dateType = new TypeSpec(DataType.TIMESTAMP);
        TypeSpec stringType = new TypeSpec(DataType.TEXT);
        TypeSpec phoneType = new TypeSpec(DataType.UDT)
          ..udtFields["prefix"] = intType
          ..udtFields["phone"] = stringType;
        TypeSpec tagType = new TypeSpec(DataType.UDT)
          ..udtFields["when"] = dateType
          ..udtFields["labels"] = new TypeSpec(DataType.LIST, valueSubType : stringType);

        TypeSpec type = new TypeSpec(DataType.UDT)
          ..udtFields["address"] = stringType
          ..udtFields["phones"] = new TypeSpec(DataType.LIST, valueSubType : phoneType)
          ..udtFields["tags"] = new TypeSpec(DataType.MAP, keySubType : stringType, valueSubType : tagType);

        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

      test("TUPLE", () {
        Object input = new Tuple.fromIterable(["Test", 3.14, new DateTime.now()]);
        TypeSpec type = new TypeSpec(DataType.TUPLE)
          ..tupleFields.add(new TypeSpec(DataType.TEXT))
          ..tupleFields.add(new TypeSpec(DataType.DOUBLE))
          ..tupleFields.add(new TypeSpec(DataType.TIMESTAMP));

        encoder.writeTypedValue('test', input, typeSpec : type, size : size);
        Object output = mock.createDecoder(encoder).readTypedValue(type, size : size);

        expect(output, equals(input));
      });

    });
  });
}