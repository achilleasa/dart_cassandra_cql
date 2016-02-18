part of dart_cassandra_cql.types;

class BatchType extends Enum<int> {
  static const BatchType LOGGED = const BatchType._(0x01);
  static const BatchType UNLOGGED = const BatchType._(0x02);
  static const BatchType COUNTER = const BatchType._(0x03);

  const BatchType._(int value) : super(value);

  String toString() => "0x${value.toRadixString(16)}";
}
