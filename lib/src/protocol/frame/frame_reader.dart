part of dart_cassandra_cql.protocol;

class FrameReader {
  StreamTransformer<Frame, Message> get transformer =>
      new StreamTransformer<Frame, Message>.fromHandlers(
          handleData: handleData,
          handleDone: handleDone,
          handleError: handleError);

  void handleError(
      Object error, StackTrace stackTrace, EventSink<Message> sink) {
    // If this is a wrapped ExceptionMessage add it to the sink; otherwise add it as an error
    if (error is ExceptionMessage) {
      sink.add(error);
    } else {
      sink.addError(error, stackTrace);
    }
  }

  void handleDone(EventSink<Message> sink) {
    sink.close();
  }

  void handleData(Frame frame, EventSink<Message> sink) {
    try {
      TypeDecoder decoder =
          new TypeDecoder.fromBuffer(frame.body, frame.getProtocolVersion());
      Message message = null;
      switch (frame.header.opcode) {
        case Opcode.AUTHENTICATE:
          message = new AuthenticateMessage.parse(decoder);
          break;
        case Opcode.AUTH_CHALLENGE:
          message = new AuthChallengeMessage.parse(decoder);
          break;
        case Opcode.AUTH_SUCCESS:
          message = new AuthSuccessMessage.parse(decoder);
          break;
        case Opcode.ERROR:
          message = new ErrorMessage.parse(decoder);
          break;
        case Opcode.READY:
          message = new ReadyMessage();
          break;
        case Opcode.RESULT:
          message = new ResultMessage.parse(decoder);
          break;
        case Opcode.EVENT:
          message = new EventMessage.parse(decoder);
          break;
        default:
          return;
      }

      // Fill in stream id
      message.streamId = frame.header.streamId;

      // Emit parsed message to next stage
      sink.add(message);
    } catch (ex, trace) {
      // Emit an exception message
      ExceptionMessage message = new ExceptionMessage(ex, trace);
      message.streamId = frame.header.streamId;
      sink.add(message);
    }
  }
}
