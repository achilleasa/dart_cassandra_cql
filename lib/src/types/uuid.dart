part of dart_cassandra_cql.types;

// Helpers
_uuidImpl.Uuid _uuidFactory = _uuidImpl.Uuid();

class Uuid {
  String value;

  Uuid([this.value]);

  /**
   * Construct a [Uuid] from a the input [bytes] list
   */
  Uuid.fromBytes(List<int> bytes) : value = _uuidFactory.unparse(bytes);

  /**
   * Create a V4 (randomised) uuid
   */
  factory Uuid.simple() {
    return Uuid(_uuidFactory.v4());
  }

  /**
   * Create a V1 (time-based) uuid
   */
  factory Uuid.timeBased() {
    return Uuid(_uuidFactory.v1());
  }

  String toString() {
    return value;
  }

  bool operator ==(other) {
    if (other is! Uuid) return false;
    return value == (other as Uuid).value;
  }

  /**
   * Convert uuid to a [Uint8List] byte list
   */

  get bytes => Uint8List.fromList(_uuidFactory.parse(value));
}
