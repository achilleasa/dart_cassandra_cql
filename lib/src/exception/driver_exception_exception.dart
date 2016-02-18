part of dart_cassandra_cql.exception;

class DriverException implements Exception {
  String message;
  StackTrace stackTrace;

  DriverException(this.message, [this.stackTrace = null]);

  String toString() {
    return "DriverException: ${message}";
  }
}
