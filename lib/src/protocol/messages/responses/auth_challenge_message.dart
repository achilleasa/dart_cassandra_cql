part of dart_cassandra_cql.protocol;

class AuthChallengeMessage extends Message {
  Uint8List challengePayload;

  AuthChallengeMessage.parse(TypeDecoder decoder)
      : super(Opcode.AUTH_CHALLENGE) {
    challengePayload = decoder.readBytes(SizeType.LONG);
  }
}
