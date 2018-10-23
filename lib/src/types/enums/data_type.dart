part of dart_cassandra_cql.types;

class DataType extends Enum<int> {
  static final RegExp _UUID_REGEX = new RegExp(
      r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
      caseSensitive: false);

  static const DataType CUSTOM = const DataType._(0x0000);
  static const DataType ASCII = const DataType._(0x0001);
  static const DataType BIGINT = const DataType._(0x0002);
  static const DataType BLOB = const DataType._(0x0003);
  static const DataType BOOLEAN = const DataType._(0x0004);
  static const DataType COUNTER = const DataType._(0x0005);
  static const DataType DECIMAL = const DataType._(0x0006);
  static const DataType DOUBLE = const DataType._(0x0007);
  static const DataType FLOAT = const DataType._(0x0008);
  static const DataType INT = const DataType._(0x0009);
  static const DataType TEXT = const DataType._(0x000a);
  static const DataType TIMESTAMP = const DataType._(0x000b);
  static const DataType UUID = const DataType._(0x000c);
  static const DataType VARCHAR = const DataType._(0x000d);
  static const DataType VARINT = const DataType._(0x000e);
  static const DataType TIMEUUID = const DataType._(0x000f);
  static const DataType INET = const DataType._(0x0010);
  static const DataType LIST = const DataType._(0x0020);
  static const DataType MAP = const DataType._(0x0021);
  static const DataType SET = const DataType._(0x0022);

  // V3 protocol (user defined types & tuples)
  static const DataType UDT = const DataType._(0x0030);
  static const DataType TUPLE = const DataType._(0x0031);

  const DataType._(int value) : super(value);

  String toString() => "0x${value.toRadixString(16)}";

  get isCollection => this == LIST || this == SET || this == MAP;

  static DataType valueOf(int value) {
    DataType fromValue = value == CUSTOM._value
        ? CUSTOM
        : value == ASCII._value
            ? ASCII
            : value == BIGINT._value
                ? BIGINT
                : value == BLOB._value
                    ? BLOB
                    : value == BOOLEAN._value
                        ? BOOLEAN
                        : value == COUNTER._value
                            ? COUNTER
                            : value == DECIMAL._value
                                ? DECIMAL
                                : value == DOUBLE._value
                                    ? DOUBLE
                                    : value == FLOAT._value
                                        ? FLOAT
                                        : value == INT._value
                                            ? INT
                                            : value == TEXT._value
                                                ? TEXT
                                                : value == TIMESTAMP._value
                                                    ? TIMESTAMP
                                                    : value == UUID._value
                                                        ? UUID
                                                        : value == VARCHAR._value
                                                            ? VARCHAR
                                                            : value ==
                                                                    VARINT
                                                                        ._value
                                                                ? VARINT
                                                                : value ==
                                                                        TIMEUUID
                                                                            ._value
                                                                    ? TIMEUUID
                                                                    : value ==
                                                                            INET._value
                                                                        ? INET
                                                                        : value ==
                                                                                LIST._value
                                                                            ? LIST
                                                                            : value == MAP._value
                                                                                ? MAP
                                                                                : value == SET._value ? SET : value == UDT._value ? UDT : value == TUPLE._value ? TUPLE : null;

    if (fromValue == null) {
      throw new ArgumentError(
          "Invalid datatype value 0x${value.toRadixString(16)}");
    }
    return fromValue;
  }

  static String nameOf(DataType value) {
    String name = value == CUSTOM
        ? "CUSTOM"
        : value == ASCII
            ? "ASCII"
            : value == BIGINT
                ? "BIGINT"
                : value == BLOB
                    ? "BLOB"
                    : value == BOOLEAN
                        ? "BOOLEAN"
                        : value == COUNTER
                            ? "COUNTER"
                            : value == DECIMAL
                                ? "DECIMAL"
                                : value == DOUBLE
                                    ? "DOUBLE"
                                    : value == FLOAT
                                        ? "FLOAT"
                                        : value == INT
                                            ? "INT"
                                            : value == TEXT
                                                ? "TEXT"
                                                : value == TIMESTAMP
                                                    ? "TIMESTAMP"
                                                    : value == UUID
                                                        ? "UUID"
                                                        : value == VARCHAR
                                                            ? "VARCHAR"
                                                            : value == VARINT
                                                                ? "VARINT"
                                                                : value ==
                                                                        TIMEUUID
                                                                    ? "TIMEUUID"
                                                                    : value ==
                                                                            INET
                                                                        ? "INET"
                                                                        : value ==
                                                                                LIST
                                                                            ? "LIST"
                                                                            : value == MAP
                                                                                ? "MAP"
                                                                                : value == SET ? "SET" : value == UDT ? "UDT" : value == TUPLE ? "TUPLE" : null;
    return name;
  }

  /**
   * Attempt to guess the correct [DataType] for the given. Returns
   * the guessed [DataType] or null if type cannot be guessed
   */

  static DataType guessForValue(Object value) {
    if (value is bool) {
      return BOOLEAN;
    } else if (value is BigInt) {
      return VARINT;
    } else if (value is int) {
      int v = value;
      return v.bitLength <= 32 ? INT : v.bitLength <= 64 ? BIGINT : VARINT;
    } else if (value is num) {
      return DOUBLE;
    } else if (value is Uuid ||
        (value is String && _UUID_REGEX.hasMatch(value))) {
      return UUID;
    } else if (value is String) {
      return VARCHAR;
    } else if (value is ByteData || value is TypedData) {
      return BLOB;
    } else if (value is DateTime) {
      return TIMESTAMP;
    } else if (value is InternetAddress) {
      return INET;
    } else if (value is Tuple) {
      return TUPLE;
    } else if (value is Set) {
      return SET;
    } else if (value is List) {
      return LIST;
    } else if (value is Map) {
      return MAP;
    }

    return null;
  }
}
