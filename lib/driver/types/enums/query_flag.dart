part of dart_cassandra_cql.types;

class QueryFlag extends Enum<int> {
  static const QueryFlag VALUES = const QueryFlag._(0x01);
  static const QueryFlag SKIP_METADATA = const QueryFlag._(0x02);
  static const QueryFlag PAGE_SIZE = const QueryFlag._(0x04);
  static const QueryFlag WITH_PAGING_STATE = const QueryFlag._(0x08);
  static const QueryFlag WITH_SERIAL_CONSISTENCY = const QueryFlag._(0x10);

  const QueryFlag._(int value) : super(value);
}
