part of dart_cassandra_cql.exception;

class ConnectionFailedException extends DriverException {
  ConnectionFailedException(String message, [StackTrace stackTrace = null])
      : super(message, stackTrace);

  String toString() {
    return "ConnectionFailedException: ${message}";
  }
}
