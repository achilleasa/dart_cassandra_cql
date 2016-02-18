part of dart_cassandra_cql.types;

/**
 * A simple typed wrapper over a standard list
 * so we can distinguish between tuples and iterables
 * during serialization
 */
class Tuple extends _wrappers.DelegatingList<Object> {
  Tuple.fromIterable(Iterable iterable) : super(iterable);
}
