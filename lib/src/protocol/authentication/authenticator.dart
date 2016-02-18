part of dart_cassandra_cql.protocol;

abstract class Authenticator {
  /**
   * Get the class of this authenticator
   */
  String get authenticatorClass;

  /**
   * Process the [challenge] sent by the server and return a [Uint8List] response
   */
  Uint8List answerChallenge(Uint8List challenge);
}
