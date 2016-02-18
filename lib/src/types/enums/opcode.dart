part of dart_cassandra_cql.types;

class Opcode extends Enum<int> {
  static const Opcode ERROR = const Opcode._(0x00);
  static const Opcode STARTUP = const Opcode._(0x01);
  static const Opcode READY = const Opcode._(0x02);
  static const Opcode AUTHENTICATE = const Opcode._(0x03);
  static const Opcode OPTIONS = const Opcode._(0x05);
  static const Opcode SUPPORTED = const Opcode._(0x06);
  static const Opcode QUERY = const Opcode._(0x07);
  static const Opcode RESULT = const Opcode._(0x08);
  static const Opcode PREPARE = const Opcode._(0x09);
  static const Opcode EXECUTE = const Opcode._(0x0a);
  static const Opcode REGISTER = const Opcode._(0x0b);
  static const Opcode EVENT = const Opcode._(0x0c);
  static const Opcode BATCH = const Opcode._(0x0d);
  static const Opcode AUTH_CHALLENGE = const Opcode._(0x0e);
  static const Opcode AUTH_RESPONSE = const Opcode._(0x0f);
  static const Opcode AUTH_SUCCESS = const Opcode._(0x10);

  const Opcode._(int value) : super(value);

  String toString() => "0x${value.toRadixString(16)}";

  static Opcode valueOf(int value) {
    Opcode fromValue = value == ERROR._value
        ? ERROR
        : value == STARTUP._value
            ? STARTUP
            : value == READY._value
                ? READY
                : value == AUTHENTICATE._value
                    ? AUTHENTICATE
                    : value == OPTIONS._value
                        ? OPTIONS
                        : value == SUPPORTED._value
                            ? SUPPORTED
                            : value == QUERY._value
                                ? QUERY
                                : value == RESULT._value
                                    ? RESULT
                                    : value == PREPARE._value
                                        ? PREPARE
                                        : value == EXECUTE._value
                                            ? EXECUTE
                                            : value == REGISTER._value
                                                ? REGISTER
                                                : value == EVENT._value
                                                    ? EVENT
                                                    : value == BATCH._value
                                                        ? BATCH
                                                        : value ==
                                                                AUTH_CHALLENGE
                                                                    ._value
                                                            ? AUTH_CHALLENGE
                                                            : value ==
                                                                    AUTH_RESPONSE
                                                                        ._value
                                                                ? AUTH_RESPONSE
                                                                : value ==
                                                                        AUTH_SUCCESS
                                                                            ._value
                                                                    ? AUTH_SUCCESS
                                                                    : null;

    if (fromValue == null) {
      throw new ArgumentError(
          "Invalid opcode value 0x${value.toRadixString(16)}");
    }
    return fromValue;
  }

  static String nameOf(Opcode value) {
    String nameValue = value == ERROR
        ? "ERROR"
        : value == STARTUP
            ? "STARTUP"
            : value == READY
                ? "READY"
                : value == AUTHENTICATE
                    ? "AUTHENTICATE"
                    : value == OPTIONS
                        ? "OPTIONS"
                        : value == SUPPORTED
                            ? "SUPPORTED"
                            : value == QUERY
                                ? "QUERY"
                                : value == RESULT
                                    ? "RESULT"
                                    : value == PREPARE
                                        ? "PREPARE"
                                        : value == EXECUTE
                                            ? "EXECUTE"
                                            : value == REGISTER
                                                ? "REGISTER"
                                                : value == EVENT
                                                    ? "EVENT"
                                                    : value == BATCH
                                                        ? "BATCH"
                                                        : value ==
                                                                AUTH_CHALLENGE
                                                            ? "AUTH_CHALLENGE"
                                                            : value ==
                                                                    AUTH_RESPONSE
                                                                ? "AUTH_RESPONSE"
                                                                : value ==
                                                                        AUTH_SUCCESS
                                                                    ? "AUTH_SUCCESS"
                                                                    : null;

    return nameValue;
  }
}
