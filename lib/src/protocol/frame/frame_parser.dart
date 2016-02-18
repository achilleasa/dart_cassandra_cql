part of dart_cassandra_cql.protocol;

class FrameParser {
  final ChunkedInputReader _inputBuffer = new ChunkedInputReader();
  FrameHeader _parsedHeader;
  Uint8List _bodyData;
  int _bodyWriteOffset = 0;
  int _headerSizeInBytes;
  ProtocolVersion _protocolVersion;

  void handleData(List<int> chunk, EventSink<Frame> sink) {
    try {
      // Append incoming chunk to input buffer
      if (chunk != null) {
        _inputBuffer.add(chunk);
      }

      // Are we extracting header bytes?
      if (_parsedHeader == null) {
        if (_headerSizeInBytes == null) {
          // Peek the first byte to figure out the version
          int version = _inputBuffer.peekNextByte();
          if (version == HeaderVersion.RESPONSE_V2.value ||
              version == HeaderVersion.REQUEST_V2.value) {
            _headerSizeInBytes = FrameHeader.SIZE_IN_BYTES_V2;
            _protocolVersion = ProtocolVersion.V2;
          } else if (version == HeaderVersion.RESPONSE_V3.value ||
              version == HeaderVersion.REQUEST_V3.value) {
            _headerSizeInBytes = FrameHeader.SIZE_IN_BYTES_V3;
            _protocolVersion = ProtocolVersion.V3;
          }
        }

        // Not enough bytes to parse header; wait till we get more
        if (_headerSizeInBytes == null ||
            _inputBuffer.length < _headerSizeInBytes) {
          return;
        }

        // Extract header bytes and parse them
        Uint8List headerBytes = new Uint8List(_headerSizeInBytes);
        _inputBuffer.read(headerBytes, _headerSizeInBytes);
        //dump = new File("${new DateTime.now()}.dump");
        //dump.writeAsBytesSync(headerBytes);
        _parsedHeader = new TypeDecoder.fromBuffer(
                new ByteData.view(headerBytes.buffer), _protocolVersion)
            .readHeader();
        headerBytes = null;

        if (_parsedHeader.length > FrameHeader.MAX_LENGTH_IN_BYTES) {
          throw new DriverException(
              "Frame size cannot be larger than ${FrameHeader.MAX_LENGTH_IN_BYTES} bytes. Attempted to read ${_parsedHeader.length} bytes");
        }

        // Allocate buffer for body and reset write offset
        _bodyData = new Uint8List(_parsedHeader.length);
        _bodyWriteOffset = 0;
      } else {
        // Copy pending body data
        _bodyWriteOffset += _inputBuffer.read(_bodyData,
            _parsedHeader.length - _bodyWriteOffset, _bodyWriteOffset);
      }

      // If we are done emit the frame to the next pipeline stage and cleanup
      if (_bodyWriteOffset == _parsedHeader.length) {
        // Ignore messages with unknown opcodes
        if (_parsedHeader.opcode != null) {
          //dump.writeAsBytesSync(_bodyData, mode : FileMode.APPEND);
          sink.add(new Frame.fromParts(
              _parsedHeader, new ByteData.view(_bodyData.buffer)));
        } else {
          throw new DriverException(
              "Unknown frame with opcode 0x${_parsedHeader.unknownOpcodeValue.toRadixString(16)} and payload size 0x${_parsedHeader.length}");
        }
        _parsedHeader = null;
        _headerSizeInBytes = null;
        _protocolVersion = null;
        _bodyData = null;
      }

      // If we have not exhausted our data buffers, trigger this method
      // again to process the remaining data before resuming processing
      if (_inputBuffer.length > 0) {
        handleData(null, sink);
      }
    } catch (e, trace) {
      // / Emit an exception message
      ExceptionMessage message = new ExceptionMessage(e,
          e is DriverException && e.stackTrace != null ? e.stackTrace : trace);
      message.streamId = _parsedHeader.streamId;

      _parsedHeader = null;
      _headerSizeInBytes = null;
      _protocolVersion = null;
      _bodyData = null;

      sink.addError(message);
    }
  }

  void handleDone(EventSink<Frame> sink) {
    sink.close();
  }

  void handleError(Object error, StackTrace stackTrace, EventSink<Frame> sink) {
    sink.addError(error, stackTrace);
  }

  StreamTransformer<List<int>, Frame> get transformer =>
      new StreamTransformer<List<int>, Frame>.fromHandlers(
          handleData: handleData,
          handleDone: handleDone,
          handleError: handleError);
}
