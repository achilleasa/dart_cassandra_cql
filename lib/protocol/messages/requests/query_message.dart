part of dart_cassandra_cql.protocol;

class QueryMessage extends Message implements RequestMessage {
  Consistency consistency;
  Consistency serialConsistency;
  String query;
  Object bindings;
  int resultPageSize;
  Uint8List pagingState;

  QueryMessage() : super(Opcode.QUERY);

  void _writeBindings(TypeEncoder encoder) {
    // Non-prepared query messages automatically expand their arguments inside the query
    // string so this is a NO-OP.

  }

  void _writeQueryParameters(TypeEncoder encoder) {
    bool emptyBindings = (bindings == null) ||
                         (bindings is Map && (bindings as Map).isEmpty) ||
                         (bindings is List && (bindings as List).isEmpty);

    int flags = 0;
    if (!emptyBindings) {
      flags |= QueryFlag.VALUES.value;
    }
    if (resultPageSize != null) {
      flags |= QueryFlag.PAGE_SIZE.value;
    }
    if (pagingState != null) {
      flags |= QueryFlag.WITH_PAGING_STATE.value;
    }
    if (serialConsistency != null) {
      flags |= QueryFlag.WITH_SERIAL_CONSISTENCY.value;
    }

    encoder
      ..writeUInt16(consistency.value)
      ..writeUint8(flags);

    if (!emptyBindings) {
      _writeBindings(encoder);
    }

    if (resultPageSize != null) {
      encoder.writeUInt32(resultPageSize);
    }

    if (pagingState != null) {
      encoder.writeBytes(pagingState, SizeType.LONG);
    }
    if (serialConsistency != null) {
      encoder.writeUInt16(serialConsistency.value);
    }

  }

  void write(TypeEncoder encoder) {

    // Write query
    encoder.writeString(query, SizeType.LONG);

    // Write query parameters
    _writeQueryParameters(encoder);
  }

}
