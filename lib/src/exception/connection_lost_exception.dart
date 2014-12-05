part of dart_cassandra_cql.exception;

class ConnectionLostException extends DriverException {
  ConnectionLostException(String message, [ StackTrace stackTrace = null]) : super(message, stackTrace);
}
