part of dart_cassandra_cql.protocol;

class ResultMetadata {
  int flags;
  Uint8List pagingState;
  Map<String, TypeSpec> colSpec;
}
