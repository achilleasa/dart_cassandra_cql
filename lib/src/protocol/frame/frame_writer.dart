part of dart_cassandra_cql.protocol;

class FrameWriter {
  TypeEncoder _typeEncoder;
  FrameHeader _header = new FrameHeader();

  FrameWriter(int streamId, ProtocolVersion protocolVersion, {TypeEncoder withEncoder : null}) {
    _header
      ..version = protocolVersion == ProtocolVersion.V2 ? HeaderVersion.REQUEST_V2 : HeaderVersion.REQUEST_V3
      ..flags = 0
      ..streamId = streamId;

    _typeEncoder = withEncoder == null
                   ? new TypeEncoder(protocolVersion)
                   : withEncoder;
  }

  int getStreamId() {
    return _header.streamId;
  }

  void writeMessage(RequestMessage message, Sink targetSink, { Compression compression}) {
    // Buffer message so we can measure its length
    message.write(_typeEncoder);

    // If compression is enabled, compress payload before filling the header
    // According to the spec, compression does not apply to the initial STARTUP message
    _header.flags = 0;
    if (compression != null && message.opcode != Opcode.STARTUP) {
      Codec<Object, Uint8List> compressionCodec = getCodec(compression.value);
      if (compressionCodec == null) {
        throw new DriverException("A compression codec needs to be registered via registerCodec() for type '${compression}'");
      }

      // Get uncompressed payload size
      int uncompressedLen = _typeEncoder.writer.lengthInBytes;

      // Concatenate all writer blocks into a single chunk and then pass it through the compression codec
      // Catch and wrap any codec exceptions
      Uint8List compressedData;
      try {
        compressedData = compressionCodec.encode(_typeEncoder.writer.joinChunks());
      } catch (e, trace) {
        throw new DriverException("An error occurred while invoking '${compression}' codec (compression): ${e}", trace);
      }

      // Replace writer blocks with compressed output and enable the compression flag for the header
      _header.flags |= HeaderFlag.COMPRESSION.value;
      _typeEncoder.writer.clear();
      _typeEncoder.writer.addLast(compressedData);
    }

    // Check for max payload size
    if (_typeEncoder.writer.lengthInBytes > FrameHeader.MAX_LENGTH_IN_BYTES) {
      _typeEncoder.writer.clear();
      throw new DriverException("Frame size cannot be larger than ${FrameHeader.MAX_LENGTH_IN_BYTES} bytes. Attempted to write ${_typeEncoder.writer.lengthInBytes} bytes");
    }

    // Allocate header buffer
    Uint8List buf = new Uint8List(_typeEncoder.protocolVersion == ProtocolVersion.V2
                                  ? FrameHeader.SIZE_IN_BYTES_V2
                                  : FrameHeader.SIZE_IN_BYTES_V3);
    ByteData headerBytes = new ByteData.view(buf.buffer);

    // Encode header
    int offset = 0;
    headerBytes
      ..setUint8(offset++, _header.version.value)
      ..setUint8(offset++, _header.flags);

    // Encode stream id (V2 uses a byte, V3 uses a short)
    if (_typeEncoder.protocolVersion == ProtocolVersion.V2) {
      headerBytes.setInt8(offset++, _header.streamId);
    } else {
      headerBytes.setInt16(offset, _header.streamId);
      offset += 2;
    }

    // Encode remaining frame data
    headerBytes
      ..setUint8(offset++, message.opcode.value)
      ..setUint32(offset++, _typeEncoder.writer.lengthInBytes);

    // Prepend the header to the writer buffer queue
    _typeEncoder.writer.addFirst(buf);

    // Dump
    //_typeEncoder.dumpToFile("frame-out.dump");

    // Pipe everything to the sink
    _typeEncoder.writer.pipe(targetSink);
  }
}

