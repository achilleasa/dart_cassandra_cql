library dart_cassandra_cql.tests.compression;

import "dart:typed_data";
import "dart:convert";

class RotConverter extends Converter<Uint8List, Uint8List> {
  final bool throwOnConvert;
  final int _key;

  const RotConverter(this._key, this.throwOnConvert);

  Uint8List convert(Uint8List input) {
    if (throwOnConvert) {
      throw new Exception("Something has gone awfully wrong...");
    }
    Uint8List result = new Uint8List(input.length);

    for (int i = 0; i < input.length; i++) {
      result[i] = (input[i] + _key) % 256;
    }

    return result;
  }
}

class MockCompressionCodec extends Codec<Uint8List, Uint8List> {
  bool throwOnEncode;
  bool throwOnDecode;

  // For our test apply ROT-13 to compress/decompress
  RotConverter _encoder;
  RotConverter _decoder;

  MockCompressionCodec(
      [this.throwOnEncode = false, this.throwOnDecode = false]) {
    _encoder = new RotConverter(13, throwOnEncode);
    _decoder = new RotConverter(-13, throwOnDecode);
  }

  Converter<Uint8List, Uint8List> get encoder {
    return _encoder;
  }

  Converter<Uint8List, Uint8List> get decoder {
    return _decoder;
  }
}
