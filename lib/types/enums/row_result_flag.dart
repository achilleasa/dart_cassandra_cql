part of dart_cassandra_cql.types;

class RowResultFlag extends Enum<int> {
  static const RowResultFlag GLOBAL_TABLE_SPEC = const RowResultFlag._(0x01);
  static const RowResultFlag HAS_MORE_PAGES = const RowResultFlag._(0x02);
  static const RowResultFlag NO_METADATA = const RowResultFlag._(0x04);

  const RowResultFlag._(int value) : super(value);

  String toString() => "0x${value.toRadixString(16)}";
}
