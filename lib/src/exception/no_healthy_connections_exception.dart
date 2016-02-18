part of dart_cassandra_cql.exception;

class NoHealthyConnectionsException extends DriverException {
  NoHealthyConnectionsException(String message, [StackTrace stackTrace = null])
      : super(message, stackTrace);

  String toString() {
    return "NoHealthyConnectionsException: ${message}";
  }
}
