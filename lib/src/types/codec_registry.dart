part of dart_cassandra_cql.types;

// Cassandra custom type codec registry
Map<String, Codec<Object, Uint8List>> _customTypeCodecs = {};

/**
 * Register a [Codec<Object, Uint8List] for handling serialization/deserialization
 * of a data type with class [typeClassName]. [typeClassName] can either be
 * a cassandra custom type fully qualified java class name or a [Compression] enum value
 * when registering a compression codec to be used for communicating with cassandra
 */
void registerCodec(String typeClassName, Codec<Object, Uint8List> codec) {
  _customTypeCodecs[typeClassName] = codec;
}

/**
 * Unregister any previously registered [Codec<Object, Uint8List] for handling serialization/deserialization
 * of data type with class [typeClassName]
 */
void unregisterCodec(String typeClassName) {
  _customTypeCodecs.remove(typeClassName);
}

/**
 * Get a [Codec<Object, Uint8List] for handling serialization/deserialization
 * of custom cassandra data type with class [typeClassName]
 */
Codec<Object, Uint8List> getCodec(String typeClassName) =>
    _customTypeCodecs[typeClassName];
