part of dart_cassandra_cql.types;

class Compression extends Enum<String> {
  static const Compression LZ4 = const Compression._("lz4");
  static const Compression SNAPPY = const Compression._("snappy");

  String toString() => value;

  const Compression._(String value) : super(value);
}
