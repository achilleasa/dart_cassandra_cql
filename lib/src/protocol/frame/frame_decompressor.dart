part of dart_cassandra_cql.protocol;

class FrameDecompressor {

  Compression _compression;

  FrameDecompressor(this._compression);

  void handleData(Frame frame, EventSink<Frame> sink) {

    if ((frame.header.flags & HeaderFlag.COMPRESSION.value) == HeaderFlag.COMPRESSION.value) {
      // Fetch compression codec
      Codec<Object, Uint8List> compressionCodec = _compression != null
                                                  ? getCodec(_compression.value)
                                                  : null;

      try {
        if (compressionCodec == null && _compression == null) {
          throw new DriverException("Server responded with an unexpected compressed frame");
        } else if (compressionCodec == null) {
          throw new DriverException("A compression codec needs to be registered via registerCodec() for type '${_compression}'");
        }

        // Decompress and replace body data. According to the spec, if the compression algorithm is LZ4
        // then the first four bytes of the payload should include its decompressed length so compliant
        // LZ4 codecs should expect this.
        Uint8List bodyData = new Uint8List.view(frame.body.buffer, 0, frame.body.lengthInBytes);

        // Generate uncompressed frame
        bodyData = compressionCodec.decode(bodyData);
        frame = new Frame.fromParts(frame.header, new ByteData.view(bodyData.buffer, 0, bodyData.lengthInBytes));
      } catch (e, trace) {
        // Wrap non-driver exceptions
        ExceptionMessage message = new ExceptionMessage(
            e is DriverException
            ? e
            : new DriverException("An error occurred while invoking '${_compression}' codec (decompression): ${e}", trace)
            , trace
        );
        message.streamId = frame.header.streamId;
        sink.addError(message);
        return;
      }
    }

    // Emit frame
    sink.add(frame);
  }

  void handleDone(EventSink<Frame> sink) {
    sink.close();
  }

  void handleError(Object error, StackTrace stackTrace, EventSink<Frame> sink) {
    sink.addError(error, stackTrace);
  }

  StreamTransformer<Frame, Frame> get transformer => new StreamTransformer<Frame, Frame>.fromHandlers(
      handleData: handleData,
      handleDone: handleDone,
      handleError: handleError
  );

}
