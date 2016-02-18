part of dart_cassandra_cql.protocol;

class AuthResponseMessage extends Message implements RequestMessage {
  Uint8List responsePayload;

  AuthResponseMessage() : super(Opcode.AUTH_RESPONSE);

  void write(TypeEncoder encoder) {
    encoder.writeBytes(responsePayload, SizeType.LONG);
  }
}
