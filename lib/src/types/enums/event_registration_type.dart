part of dart_cassandra_cql.types;

class EventRegistrationType extends Enum<String> {
  static const EventRegistrationType TOPOLOGY_CHANGE = const EventRegistrationType._("TOPOLOGY_CHANGE");
  static const EventRegistrationType STATUS_CHANGE = const EventRegistrationType._("STATUS_CHANGE");
  static const EventRegistrationType SCHEMA_CHANGE = const EventRegistrationType._("SCHEMA_CHANGE");

  const EventRegistrationType._(String value) : super(value);

  String toString() => value;

  static EventRegistrationType valueOf(String value) {
    EventRegistrationType fromValue = value == TOPOLOGY_CHANGE._value ? TOPOLOGY_CHANGE :
                                      value == STATUS_CHANGE._value ? STATUS_CHANGE :
                                      value == SCHEMA_CHANGE._value ? SCHEMA_CHANGE : null;

    if (fromValue == null) {
      throw new ArgumentError("Invalid event registration value ${value}");
    }
    return fromValue;
  }

  static String nameOf(EventRegistrationType value) {
    String nameValue = value == TOPOLOGY_CHANGE ? "TOPOLOGY_CHANGE" :
                       value == STATUS_CHANGE ? "STATUS_CHANGE" :
                       value == SCHEMA_CHANGE ? "SCHEMA_CHANGE" : null;

    return nameValue;
  }
}
