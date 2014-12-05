part of dart_cassandra_cql.protocol;

class AuthenticateMessage extends Message {

  String authenticatorClass;

  AuthenticateMessage.parse(TypeDecoder decoder) : super(Opcode.AUTHENTICATE){

    authenticatorClass = decoder.readString(SizeType.SHORT);

  }

  Uint8List get challengePayload => null;
}
