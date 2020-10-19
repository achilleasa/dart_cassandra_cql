part of dart_cassandra_cql.types;

class EventType extends Enum<String> {
  static const EventType NODE_ADDED = const EventType._("NEW_NODE");
  static const EventType NODE_REMOVED = const EventType._("REMOVED_NODE");
  static const EventType NODE_UP = const EventType._("UP");
  static const EventType NODE_DOWN = const EventType._("DOWN");
  static const EventType SCHEMA_CREATED = const EventType._("CREATED");
  static const EventType SCHEMA_UPDATED = const EventType._("UPDATED");
  static const EventType SCHEMA_DROPPED = const EventType._("DROPPED");

  const EventType._(String value) : super(value);

  String toString() => value;

  static EventType valueOf(String value) {
    EventType fromValue = value == NODE_ADDED.value
        ? NODE_ADDED
        : value == NODE_REMOVED.value
            ? NODE_REMOVED
            : value == NODE_UP.value
                ? NODE_UP
                : value == NODE_DOWN.value
                    ? NODE_DOWN
                    : value == SCHEMA_CREATED.value
                        ? SCHEMA_CREATED
                        : value == SCHEMA_UPDATED.value
                            ? SCHEMA_UPDATED
                            : value == SCHEMA_DROPPED.value
                                ? SCHEMA_DROPPED
                                : null;

    if (fromValue == null) {
      throw ArgumentError("Invalid event value ${value}");
    }
    return fromValue;
  }

  static String nameOf(EventType value) {
    String nameValue = value == NODE_ADDED
        ? "NODE_ADDED"
        : value == NODE_REMOVED
            ? "NODE_REMOVED"
            : value == NODE_UP
                ? "NODE_UP"
                : value == NODE_DOWN
                    ? "NODE_DOWN"
                    : value == SCHEMA_CREATED
                        ? "SCHEMA_CREATED"
                        : value == SCHEMA_UPDATED
                            ? "SCHEMA_UPDATED"
                            : value == SCHEMA_DROPPED ? "SCHEMA_DROPPED" : null;

    return nameValue;
  }
}
