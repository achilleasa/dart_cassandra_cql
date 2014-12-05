part of dart_cassandra_cql.exception;

class StreamReservationException extends DriverException {
  StreamReservationException(String message, [ StackTrace stackTrace = null]) : super(message, stackTrace);

  String toString() {
    return "StreamReservationException: ${message}";
  }
}
