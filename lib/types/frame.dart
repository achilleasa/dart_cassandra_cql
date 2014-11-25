part of dart_cassandra_cql.types;

class Frame {

  final FrameHeader header;
  final ByteData body;

  const Frame.fromParts(FrameHeader this.header, ByteData this.body);

  ProtocolVersion getProtocolVersion() {
    if (header == null) {
      return null;
    }

    if (header.version == HeaderVersion.REQUEST_V2 || header.version == HeaderVersion.RESPONSE_V2) {
      return ProtocolVersion.V2;
    } else if (header.version == HeaderVersion.REQUEST_V3 || header.version == HeaderVersion.RESPONSE_V3) {
      return ProtocolVersion.V3;
    }

    return null;
  }

}

