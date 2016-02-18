part of dart_cassandra_cql.protocol;

class PrepareMessage extends Message implements RequestMessage {
  String query;

  PrepareMessage() : super(Opcode.PREPARE);

  void write(TypeEncoder encoder) {
    // Send the query as a long string
    encoder.writeString(query, SizeType.LONG);
  }
}
