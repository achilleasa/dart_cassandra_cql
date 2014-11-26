part of dart_cassandra_cql.protocol;

class StartupMessage extends Message implements RequestMessage {
  String cqlVersion;
  Compression compression;

  StartupMessage() : super(Opcode.STARTUP);

  void write(TypeEncoder encoder) {

    // Write message contents
    Map<String, String> params = {
        "CQL_VERSION" : cqlVersion
    };
    if (compression != null) {
      params["COMPRESSION"] = compression.value;
    }

    encoder.writeStringMap(params, SizeType.SHORT);
  }
}
