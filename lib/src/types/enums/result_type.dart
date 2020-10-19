part of dart_cassandra_cql.types;

class ResultType extends Enum<int> {
  static const ResultType VOID = const ResultType._(0x01);
  static const ResultType ROWS = const ResultType._(0x02);
  static const ResultType SET_KEYSPACE = const ResultType._(0x03);
  static const ResultType PREPARED = const ResultType._(0x04);
  static const ResultType SCHEMA_CHANGE = const ResultType._(0x05);

  const ResultType._(int value) : super(value);

  String toString() => "0x${value.toRadixString(16)}";

  static ResultType valueOf(int value) {
    ResultType fromValue = value == VOID._value
        ? VOID
        : value == ROWS._value
            ? ROWS
            : value == SET_KEYSPACE._value
                ? SET_KEYSPACE
                : value == PREPARED._value
                    ? PREPARED
                    : value == SCHEMA_CHANGE._value ? SCHEMA_CHANGE : null;

    if (fromValue == null) {
      throw ArgumentError(
          "Invalid result type value 0x${value.toRadixString(16)}");
    }
    return fromValue;
  }
}
