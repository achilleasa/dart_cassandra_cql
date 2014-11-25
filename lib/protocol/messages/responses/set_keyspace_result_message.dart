part of dart_cassandra_cql.protocol;

class SetKeyspaceResultMessage extends ResultMessage {
  String keyspace;

  SetKeyspaceResultMessage.parse(TypeDecoder decoder){
    keyspace = decoder.readString(SizeType.SHORT);
  }
}