part of dart_cassandra_cql.protocol;

class SchemaChangeResultMessage extends ResultMessage {
  String change;
  String keyspace;
  String table;

  SchemaChangeResultMessage.parse(TypeDecoder decoder){
    change = decoder.readString(SizeType.SHORT);
    keyspace = decoder.readString(SizeType.SHORT);
    table = decoder.readString(SizeType.SHORT);
  }
}