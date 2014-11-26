part of dart_cassandra_cql.protocol;

/**
 * The [ExceptionMessage] is used for reporting exceptions
 * caught by the [FrameReader]
 */
class ExceptionMessage extends Message {
  dynamic exception;
  StackTrace stackTrace;

  ExceptionMessage(this.exception, this.stackTrace) : super( Opcode.ERROR );
}