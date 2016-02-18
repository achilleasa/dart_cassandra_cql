part of dart_cassandra_cql.types;

class HeaderVersion extends Enum<int> {
  static const HeaderVersion REQUEST_V2 = const HeaderVersion._(0x02);
  static const HeaderVersion REQUEST_V3 = const HeaderVersion._(0x03);
  static const HeaderVersion RESPONSE_V2 = const HeaderVersion._(0x82);
  static const HeaderVersion RESPONSE_V3 = const HeaderVersion._(0x83);

  const HeaderVersion._(int value) : super(value);

  String toString() => "0x${value.toRadixString(16)}";

  static HeaderVersion valueOf(int value) {
    HeaderVersion fromValue = value == REQUEST_V2._value
        ? REQUEST_V2
        : value == RESPONSE_V2._value
            ? RESPONSE_V2
            : value == REQUEST_V3._value
                ? REQUEST_V3
                : value == RESPONSE_V3._value ? RESPONSE_V3 : null;

    if (fromValue == null) {
      throw new ArgumentError(
          "Invalid version value 0x${value.toRadixString(16)}");
    }
    return fromValue;
  }
}
