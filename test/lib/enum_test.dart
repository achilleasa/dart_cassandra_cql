library dart_cassandra_cql.tests.enums;

import "dart:mirrors";
import "package:test/test.dart";
import '../../lib/src/types.dart';

Type _getMethodArgType(MethodMirror methodMirror) {
  ParameterMirror paramMirror = methodMirror.parameters.first;
  return paramMirror.type.reflectedType;
}

Type _getGenericType(ClassMirror classMirror) {
  return classMirror.superclass.typeArguments.first.reflectedType;
}

main({bool enableLogger: true}) {
  List enumClasses = [
    BatchType,
    Consistency,
    DataType,
    ErrorCode,
    EventRegistrationType,
    EventType,
    HeaderFlag,
    HeaderVersion,
    Opcode,
    ProtocolVersion,
    QueryFlag,
    ResultType,
    RowResultFlag,
    Compression
  ];

  for (Type enumClass in enumClasses) {
    ClassMirror cm = reflectClass(enumClass);

    MethodMirror valueOfMirror = null;
    MethodMirror nameOfMirror = null;
    MethodMirror toStringMirror = null;

    // Run a first pass to detect which methods we can use
    cm.declarations.forEach((Symbol enumSymbol, declarationMirror) {
      String declName = enumSymbol
          .toString()
          .replaceAll("Symbol(\"", "")
          .replaceAll("\")", "");
      if (declarationMirror is MethodMirror) {
        if (declName == "valueOf") {
          valueOfMirror = declarationMirror;
        } else if (declName == "nameOf") {
          nameOfMirror = declarationMirror;
        } else if (declName == "toString") {
          toStringMirror = declarationMirror;
        }
      }
    });

    // Nothing to test here...
    if (nameOfMirror == null &&
        valueOfMirror == null &&
        toStringMirror == null) {
      continue;
    }

    group("${enumClass.toString()}:", () {
      // Generate tests for exceptions
      if (valueOfMirror != null) {
        group("Exceptions:", () {
          Object junkValue = _getMethodArgType(valueOfMirror) == String
              ? "BADFOOD"
              : 0xBADF00D;

          Object formattedJunkValue =
              junkValue is String ? "\"BADFOOD\"" : "0xBADF00D";

          test("valueOf(${formattedJunkValue}) throws ArgumentError", () {
            expect(() => cm.invoke(#valueOf, [junkValue]).reflectee,
                throwsArgumentError);
          });
        });
      }

      // Generate tests depending on which methods are available
      cm.declarations.forEach((Symbol enumSymbol, declarationMirror) {
        String declName = enumSymbol
            .toString()
            .replaceAll("Symbol(\"", "")
            .replaceAll("\")", "");

        // Only process public Enum instances
        if (declarationMirror is MethodMirror || declarationMirror.isPrivate) {
          return;
        }

        // Build group for enum value
        group("${declName}:", () {
          // Generate valueOf tests
          if (valueOfMirror != null) {
            test("valueOf(${declName}.value) == ${declName}", () {
              dynamic staticEnumInstance = cm.getField(enumSymbol).reflectee;
              dynamic enumValue =
                  cm.getField(enumSymbol).getField(#value).reflectee;
              expect(cm.invoke(#valueOf, [enumValue]).reflectee,
                  equals(staticEnumInstance));
            });
          }

          // Generate nameOf tests
          if (nameOfMirror != null) {
            test("nameOf(${declName}) == \"${declName}\"", () {
              dynamic staticEnumInstance = cm.getField(enumSymbol).reflectee;
              expect(cm.invoke(#nameOf, [staticEnumInstance]).reflectee,
                  equals(declName));
            });
          }

          // Generate toString tests
          if (toStringMirror != null) {
            test("${declName}.toString()", () {
              Enum<Object> staticEnumInstance =
                  cm.getField(enumSymbol).reflectee;
              Object enumValue = staticEnumInstance.value;

              Object expectedValue = _getGenericType(cm) == String
                  ? enumValue
                  : "0x${(enumValue as int).toRadixString(16)}";

              expect(staticEnumInstance.toString(), equals(expectedValue));
            });
          }
        });
      });
    });
  }
}
