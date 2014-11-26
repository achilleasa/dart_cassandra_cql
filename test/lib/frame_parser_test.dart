library dart_cassandra_cql.tests.frame_grabber;

import "dart:async";
import "package:unittest/unittest.dart";
import "mocks/mocks.dart" as mock;

import "../../lib/types.dart";
import "../../lib/protocol.dart";
import "../../lib/exceptions.dart";

main({bool enableLogger : true}) {
  if( enableLogger ){
    mock.initLogger();
  }

  FrameWriter writer;
  StreamController streamController;
  Stream frameGrabber;

  group("Frame parser (V2):", () {

    setUp(() {
      writer = new FrameWriter(0, ProtocolVersion.V2);
      streamController = new StreamController(sync : false);
      frameGrabber = streamController.stream.transform(new FrameParser().transformer);
    });

    test("READY message", () {

      frameGrabber.listen(expectAsync((Frame frame) {
        expect(frame.header.opcode, equals(Opcode.READY));
      }));
      mock.writeMessage(
          streamController
          , Opcode.READY.value
          , protocolVersion: ProtocolVersion.V2
          , headerVersion : HeaderVersion.RESPONSE_V2
      );
    });

    test("EVENT message (-1 streamId)", () {

      frameGrabber.listen(expectAsync((Frame frame) {
        expect(frame.header.opcode, equals(Opcode.EVENT));
        expect(frame.header.streamId, equals(-1));
      }));
      mock.writeMessage(
          streamController
          , Opcode.EVENT.value
          , protocolVersion: ProtocolVersion.V2
          , headerVersion : HeaderVersion.RESPONSE_V2
          , streamId : -1
      );
    });

    test("InvalidFrame exception (illegal opcode)", () {
      var error;
      Timer.run(expectAsync(() {
        expect(error, new isInstanceOf<ExceptionMessage>());
        expect(
            (error.exception as DriverException).message,
            equals("Unknown frame with opcode 0x${0xFF.toRadixString(16)} and payload size 0x0")
        );
      }));

      runZoned(() {
        frameGrabber.listen(((Frame frame) {
          throw new Exception("Should not have parsed ${frame}");
        }));
        mock.writeMessage(
            streamController
            , 0xFF
            , protocolVersion: ProtocolVersion.V2
            , headerVersion : HeaderVersion.RESPONSE_V2
        );
      }, onError: (e) {
        error = e;
      });
    });

    test("InvalidFrame exception (illegal length)", () {
      var error;
      Timer.run(expectAsync(() {
        expect(error, new isInstanceOf<ExceptionMessage>());
        expect(
            (error.exception as DriverException).message,
            equals("Frame size cannot be larger than ${FrameHeader.MAX_LENGTH_IN_BYTES} bytes. Attempted to read ${FrameHeader.MAX_LENGTH_IN_BYTES + 1} bytes")
        );
      }));

      runZoned(() {
        frameGrabber.listen(((Frame frame) {
          throw new Exception("Should not have parsed ${frame}");
        }));
        mock.writeMessage(
            streamController
            , Opcode.READY.value
            , overrideLength : FrameHeader.MAX_LENGTH_IN_BYTES + 1
            , protocolVersion: ProtocolVersion.V2
            , headerVersion : HeaderVersion.RESPONSE_V2
        );
      }, onError: (e) {
        error = e;
      });
    });
  });

  group("Frame parser (V3):", () {

    setUp(() {
      writer = new FrameWriter(0, ProtocolVersion.V3);
      streamController = new StreamController(sync : false);
      frameGrabber = streamController.stream.transform(new FrameParser().transformer);
    });

    test("READY message", () {

      frameGrabber.listen(expectAsync((Frame frame) {
        expect(frame.header.opcode, equals(Opcode.READY));
      }));
      mock.writeMessage(
          streamController
          , Opcode.READY.value
          , protocolVersion: ProtocolVersion.V2
          , headerVersion : HeaderVersion.RESPONSE_V2
      );
    });

    test("EVENT message (-1 streamId)", () {

      frameGrabber.listen(expectAsync((Frame frame) {
        expect(frame.header.opcode, equals(Opcode.EVENT));
        expect(frame.header.streamId, equals(-1));
      }));
      mock.writeMessage(
          streamController
          , Opcode.EVENT.value
          , protocolVersion: ProtocolVersion.V2
          , headerVersion : HeaderVersion.RESPONSE_V2
          , streamId : -1
      );
    });

    test("InvalidFrame exception (illegal opcode)", () {
      var error;
      Timer.run(expectAsync(() {
        expect(error, new isInstanceOf<ExceptionMessage>());
        expect(
            (error.exception as DriverException).message,
            equals("Unknown frame with opcode 0x${0xFF.toRadixString(16)} and payload size 0x0")
        );
      }));

      runZoned(() {
        frameGrabber.listen(((Frame frame) {
          throw new Exception("Should not have parsed ${frame}");
        }));
        mock.writeMessage(
            streamController
            , 0xFF
            , protocolVersion: ProtocolVersion.V2
            , headerVersion : HeaderVersion.RESPONSE_V2
        );
      }, onError: (e) {
        error = e;
      });
    });

    test("InvalidFrame exception (illegal length)", () {
      var error;
      Timer.run(expectAsync(() {
        expect(error, new isInstanceOf<ExceptionMessage>());
        expect(
            (error.exception as DriverException).message,
            equals("Frame size cannot be larger than ${FrameHeader.MAX_LENGTH_IN_BYTES} bytes. Attempted to read ${FrameHeader.MAX_LENGTH_IN_BYTES + 1} bytes")
        );
      }));

      runZoned(() {
        frameGrabber.listen(((Frame frame) {
          throw new Exception("Should not have parsed ${frame}");
        }));
        mock.writeMessage(
            streamController
            , Opcode.READY.value
            , overrideLength : FrameHeader.MAX_LENGTH_IN_BYTES + 1
            , protocolVersion: ProtocolVersion.V2
            , headerVersion : HeaderVersion.RESPONSE_V2
        );
      }, onError: (e) {
        error = e;
      });
    });
  });

}