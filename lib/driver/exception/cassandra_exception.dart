part of dart_cassandra_cql.exception;

class CassandraException extends DriverException {

  CassandraException(String message, [ StackTrace stackTrace = null]) : super(message, stackTrace);

  String toString() {
    return "CassandraException: ${message}";
  }
}
