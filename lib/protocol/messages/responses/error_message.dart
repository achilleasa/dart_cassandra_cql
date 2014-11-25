part of dart_cassandra_cql.protocol;

class ErrorMessage extends Message {

  ErrorCode code;

  String message;

  ErrorMessage.parse(TypeDecoder decoder) : super(Opcode.ERROR){
    // Parse message common
    code = ErrorCode.valueOf(decoder.readUInt());
    message = decoder.readString(SizeType.SHORT);

    // Parse additional error-specific details
    switch (code) {
      case ErrorCode.UNAVAILABLE:
        String consistency = Consistency.nameOf(decoder.readConsistency());
        int required = decoder.readUInt();
        int alive = decoder.readUInt();
        message += "Not enough nodes available to answer the query with ${consistency} consistency. Required ${required} nodes but found ${alive} alive";
        break;
      case ErrorCode.WRITE_TIMEOUT:
        String consistency = Consistency.nameOf(decoder.readConsistency());
        int received = decoder.readUInt();
        int required = decoder.readUInt();
        String writeType = decoder.readString(SizeType.SHORT);
        message += "Timeout during a write request with ${consistency} consistency. Received ${received}/${required} ACK for a ${writeType} write";
        break;
      case ErrorCode.READ_TIMEOUT:
        String consistency = Consistency.nameOf(decoder.readConsistency());
        int received = decoder.readUInt();
        int required = decoder.readUInt();
        String writeType = decoder.readString(SizeType.SHORT);
        bool dataPresent = decoder.readByte() != 0;
        message += "Timeout during a read request with ${consistency} consistency. Received ${received}/${required} responses. The replica asked for the data ${dataPresent ? "HAS" : "has NOT"} responded";
        break;
      case ErrorCode.ALREADY_EXISTS:
        String keyspace = decoder.readString(SizeType.SHORT);
        String table = decoder.readString(SizeType.SHORT);

        message += table.isEmpty
                   ? "Keyspace ${keyspace} already exists"
                   : "Table ${table} in keyspace ${keyspace} already exists";
        break;
      case ErrorCode.UNPREPARED:
        int id = decoder.readShort();
        message += "Unknown prepared query with id ${id}";
        break;
    }

  }
}
