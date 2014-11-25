part of dart_cassandra_cql.protocol;

class EventMessage extends Message {
  EventRegistrationType type;
  EventType subType;

  // Filled in for topology/status messages
  InternetAddress address;
  int port;

  // Filled in for schema messages
  String keyspace;
  String changedTable;

  // V3-only
  String changedType;

  EventMessage.parse(TypeDecoder decoder) : super(Opcode.EVENT){
    type = EventRegistrationType.valueOf(decoder.readString(SizeType.SHORT));
    subType = EventType.valueOf(decoder.readString(SizeType.SHORT));

    switch (type) {
      case EventRegistrationType.TOPOLOGY_CHANGE:
      case EventRegistrationType.STATUS_CHANGE:
        address = decoder.readTypedValue(new TypeSpec(DataType.INET), size : SizeType.BYTE);
        port = decoder.readInt();
        break;
      case EventRegistrationType.SCHEMA_CHANGE:
        switch (decoder.protocolVersion) {
          case ProtocolVersion.V2:

            keyspace = decoder.readString(SizeType.SHORT);

            // According to the spec, this should be an empty string if only the keyspace changed
            String tableName = decoder.readString(SizeType.SHORT);
            changedTable = tableName == null || tableName.isEmpty
                           ? null
                           : tableName;

            break;
          case ProtocolVersion.V3:

            String target = decoder.readString(SizeType.SHORT);
            keyspace = decoder.readString(SizeType.SHORT);

            switch (target) {
              case "TABLE":
                changedTable = decoder.readString(SizeType.SHORT);
                break;
              case "TYPE":
                changedType = decoder.readString(SizeType.SHORT);
                break;
            }
        }
        break;
    }
  }
}
