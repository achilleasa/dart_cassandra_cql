part of dart_cassandra_cql.types;

class ProtocolVersion extends Enum<int> {
  static const ProtocolVersion V2 = const ProtocolVersion._(0x02);
  static const ProtocolVersion V3 = const ProtocolVersion._(0x03);

  const ProtocolVersion._(int value) : super(value);

  String toString() => "0x${value.toRadixString(16)}";
}
