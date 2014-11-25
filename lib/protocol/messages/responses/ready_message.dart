part of dart_cassandra_cql.protocol;

class ReadyMessage extends Message {
  ReadyMessage() : super(Opcode.READY);
}
