part of dart_cassandra_cql.types;

class FrameHeader {

  static const int SIZE_IN_BYTES_V2 = 8;
  static const int SIZE_IN_BYTES_V3 = 9;
  static const int MAX_LENGTH_IN_BYTES = 268435456; // 256MB (see spec)

  HeaderVersion version;
  int flags = 0;
  int streamId = 0;
  Opcode opcode;
  int length;

  // If the message contains an unknown opcode that cannot be parsed
  // as an Opcode enum, we store it here for logging
  int unknownOpcodeValue;
}
