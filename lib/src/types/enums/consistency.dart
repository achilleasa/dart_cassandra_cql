part of dart_cassandra_cql.types;

class Consistency extends Enum<int> {
  static const Consistency ANY = const Consistency._(0x00);
  static const Consistency ONE = const Consistency._(0x01);
  static const Consistency TWO = const Consistency._(0x02);
  static const Consistency THREE = const Consistency._(0x03);
  static const Consistency QUORUM = const Consistency._(0x04);
  static const Consistency ALL = const Consistency._(0x05);
  static const Consistency LOCAL_QUORUM = const Consistency._(0x06);
  static const Consistency EACH_QUORUM = const Consistency._(0x07);
  static const Consistency SERIAL = const Consistency._(0x08);
  static const Consistency LOCAL_SERIAL = const Consistency._(0x09);
  static const Consistency LOCAL_ONE = const Consistency._(0x0A);

  const Consistency._(int value) : super(value);

  String toString() => "0x${value.toRadixString(16)}";

  static Consistency valueOf(int value) {
    Consistency fromValue = value == ANY._value
        ? ANY
        : value == ONE._value
            ? ONE
            : value == TWO._value
                ? TWO
                : value == THREE._value
                    ? THREE
                    : value == QUORUM._value
                        ? QUORUM
                        : value == ALL._value
                            ? ALL
                            : value == LOCAL_QUORUM._value
                                ? LOCAL_QUORUM
                                : value == EACH_QUORUM._value
                                    ? EACH_QUORUM
                                    : value == SERIAL._value
                                        ? SERIAL
                                        : value == LOCAL_SERIAL._value
                                            ? LOCAL_SERIAL
                                            : value == LOCAL_ONE._value
                                                ? LOCAL_ONE
                                                : null;

    if (fromValue == null) {
      throw ArgumentError(
          "Invalid consistency value 0x${value.toRadixString(16)}");
    }
    return fromValue;
  }

  static String nameOf(Consistency value) {
    String name = value == ANY
        ? "ANY"
        : value == ONE
            ? "ONE"
            : value == TWO
                ? "TWO"
                : value == THREE
                    ? "THREE"
                    : value == QUORUM
                        ? "QUORUM"
                        : value == ALL
                            ? "ALL"
                            : value == LOCAL_QUORUM
                                ? "LOCAL_QUORUM"
                                : value == EACH_QUORUM
                                    ? "EACH_QUORUM"
                                    : value == SERIAL
                                        ? "SERIAL"
                                        : value == LOCAL_SERIAL
                                            ? "LOCAL_SERIAL"
                                            : value == LOCAL_ONE
                                                ? "LOCAL_ONE"
                                                : null;

    return name;
  }
}
