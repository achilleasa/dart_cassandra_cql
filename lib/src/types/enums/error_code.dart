part of dart_cassandra_cql.types;

class ErrorCode extends Enum<int> {
  static const ErrorCode SERVER_ERROR = const ErrorCode._(0x0000);
  static const ErrorCode PROTOCOL_ERROR = const ErrorCode._(0x000A);
  static const ErrorCode BAD_CREDENTIALS = const ErrorCode._(0x0100);
  static const ErrorCode UNAVAILABLE = const ErrorCode._(0x1000);
  static const ErrorCode OVERLOADED = const ErrorCode._(0x1001);
  static const ErrorCode IS_BOOTSTRAPPING = const ErrorCode._(0x1002);
  static const ErrorCode TRUNCATE_ERROR = const ErrorCode._(0x1003);
  static const ErrorCode WRITE_TIMEOUT = const ErrorCode._(0x1100);
  static const ErrorCode READ_TIMEOUT = const ErrorCode._(0x1200);
  static const ErrorCode SYNTAX_ERROR = const ErrorCode._(0x2000);
  static const ErrorCode UNAUTHORIZED = const ErrorCode._(0x2100);
  static const ErrorCode INVALID = const ErrorCode._(0x2200);
  static const ErrorCode CONFIG_ERROR = const ErrorCode._(0x2300);
  static const ErrorCode ALREADY_EXISTS = const ErrorCode._(0x2400);
  static const ErrorCode UNPREPARED = const ErrorCode._(0x2500);

  const ErrorCode._(int value) : super(value);

  String toString() => "0x${value.toRadixString(16)}";

  static ErrorCode valueOf(int value) {
    ErrorCode fromValue = value == SERVER_ERROR._value
        ? SERVER_ERROR
        : value == PROTOCOL_ERROR._value
            ? PROTOCOL_ERROR
            : value == BAD_CREDENTIALS._value
                ? BAD_CREDENTIALS
                : value == UNAVAILABLE._value
                    ? UNAVAILABLE
                    : value == OVERLOADED._value
                        ? OVERLOADED
                        : value == IS_BOOTSTRAPPING._value
                            ? IS_BOOTSTRAPPING
                            : value == TRUNCATE_ERROR._value
                                ? TRUNCATE_ERROR
                                : value == WRITE_TIMEOUT._value
                                    ? WRITE_TIMEOUT
                                    : value == READ_TIMEOUT._value
                                        ? READ_TIMEOUT
                                        : value == SYNTAX_ERROR._value
                                            ? SYNTAX_ERROR
                                            : value == UNAUTHORIZED._value
                                                ? UNAUTHORIZED
                                                : value == INVALID._value
                                                    ? INVALID
                                                    : value ==
                                                            CONFIG_ERROR._value
                                                        ? CONFIG_ERROR
                                                        : value ==
                                                                ALREADY_EXISTS
                                                                    ._value
                                                            ? ALREADY_EXISTS
                                                            : value ==
                                                                    UNPREPARED
                                                                        ._value
                                                                ? UNPREPARED
                                                                : null;

    if (fromValue == null) {
      throw new ArgumentError(
          "Invalid error code value 0x${value.toRadixString(16)}");
    }
    return fromValue;
  }
}
