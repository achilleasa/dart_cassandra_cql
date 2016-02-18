part of dart_cassandra_cql.protocol;

class PreparedResultMessage extends ResultMessage {
  Uint8List queryId;
  ResultMetadata metadata;

  // Extra data for looking up connections to the node that prepared this query
  String host;
  int port;

  PreparedResultMessage.parse(TypeDecoder decoder) {
    queryId = decoder.readBytes(SizeType.SHORT);
    metadata = _parseMetadata(decoder);
  }
}
