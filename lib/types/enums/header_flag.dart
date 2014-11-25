part of dart_cassandra_cql.types;

class HeaderFlag extends Enum<int> {
  static const HeaderFlag COMPRESSION = const HeaderFlag._(0x01);
  static const HeaderFlag TRACING = const HeaderFlag._(0x02);

  const HeaderFlag._(int value) : super(value);
}
