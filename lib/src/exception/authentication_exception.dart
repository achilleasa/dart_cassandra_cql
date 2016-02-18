part of dart_cassandra_cql.exception;

class AuthenticationException extends DriverException {
  AuthenticationException(String message, [StackTrace stackTrace = null])
      : super(message, stackTrace);

  String toString() {
    return "AuthenticationException: ${message}";
  }
}
