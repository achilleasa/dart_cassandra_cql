part of dart_cassandra_cql.protocol;

class BatchMessage extends Message implements RequestMessage {
  BatchType type;
  Consistency consistency;
  Consistency serialConsistency;
  List<Query> queryList;

  BatchMessage() : super(Opcode.BATCH);

  void write(TypeEncoder encoder) {
    // Write batch type and number of queries
    encoder.writeUint8(type.value);

    // V3 includes a flags byte
    if (encoder.protocolVersion == ProtocolVersion.V3) {
      encoder.writeUint8(serialConsistency != null ? 0x10 : 0x00);
    }

    encoder.writeUInt16(queryList.length);

    // Write each query
    queryList.forEach((Query query) {
      // Not prepared
      encoder.writeUint8(0);

      // Write expanded query
      encoder.writeString(query.expandedQuery, SizeType.LONG);

      // As the query is expanded we have 0 args to supply
      encoder.writeUInt16(0);
    });

    // Write consistency level
    encoder.writeUInt16(consistency.value);

    // V3 includes serial_consistency
    if (serialConsistency != null) {
      encoder.writeUInt16(serialConsistency.value);
    }
  }
}
