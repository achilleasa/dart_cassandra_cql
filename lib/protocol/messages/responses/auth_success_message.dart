part of dart_cassandra_cql.protocol;

class AuthSuccessMessage extends Message {

  Uint8List token;

  AuthSuccessMessage.parse(TypeDecoder decoder) : super(Opcode.AUTH_SUCCESS){

    token = decoder.readBytes(SizeType.LONG);

  }
}
