part of dart_cassandra_cql.protocol;

class RegisterMessage extends Message implements RequestMessage {
  List<EventRegistrationType> eventTypes;

  RegisterMessage() : super(Opcode.REGISTER);

  void write(TypeEncoder encoder) {

    encoder.writeStringList(eventTypes, SizeType.SHORT);
  }
}
