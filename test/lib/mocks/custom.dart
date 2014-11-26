library dart_cassandra_cql.tests.custom;

import "dart:typed_data";
import "dart:convert";
import '../../../lib/driver/types.dart';

class CustomJson implements CustomType {
  Map payload;

  String get customTypeClass => "com.achilleasa.cassandra.cqltypes.Json";

  CustomJson(this.payload);
}

class CustomJsonEncoder extends Converter<CustomJson, Uint8List> {

  Uint8List convert(CustomJson input) {
    return input.payload == null
           ? null
           : new Uint8List.fromList(JSON.encode(input.payload).codeUnits);
  }
}

class CustomJsonDecoder extends Converter<Uint8List, CustomJson> {

  CustomJson convert(Uint8List input) {
    Map payload = input == null
                  ? null
                  : new JsonDecoder().convert(UTF8.decode(input));

    return new CustomJson(payload);
  }
}

class CustomJsonCodec extends Codec<CustomJson, Uint8List> {

  final CustomJsonEncoder _encoder = new CustomJsonEncoder();
  final CustomJsonDecoder _decoder = new CustomJsonDecoder();

  Converter<CustomJson, Uint8List> get encoder {
    return _encoder;
  }

  Converter<Uint8List, CustomJson> get decoder {
    return _decoder;
  }

}