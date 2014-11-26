library dart_cassandra_cql.tests.frame_writer;

import "package:unittest/unittest.dart";
import "mocks/mocks.dart" as mock;

import "../../lib/types.dart";
import "../../lib/protocol.dart";
import "../../lib/stream.dart";
import "../../lib/exceptions.dart";

main({bool enableLogger : true}) {
  if( enableLogger ){
    mock.initLogger();
  }

  FrameWriter frameWriter;
  mock.MockChunkedOutputWriter mockOutputWriter = new mock.MockChunkedOutputWriter();

  group("Frame writer:", () {

    setUp(() {
      frameWriter = new FrameWriter(0
      , ProtocolVersion.V2
      , withEncoder : new TypeEncoder(ProtocolVersion.V2, withWriter : mockOutputWriter)
      );
    });

    test("InvalidFrame exception (illegal length)", () {
      var error;
      try {
        mockOutputWriter.forcedLengthInBytes = FrameHeader.MAX_LENGTH_IN_BYTES + 1;
        frameWriter.writeMessage(new StartupMessage(), null);
      } on Exception catch (e) {
        error = e;
      }
      expect(error, new isInstanceOf<DriverException>());
      expect(
          (error as DriverException).message,
          equals("Frame size cannot be larger than ${FrameHeader.MAX_LENGTH_IN_BYTES} bytes. Attempted to write ${FrameHeader.MAX_LENGTH_IN_BYTES + 1} bytes")
      );
    });
  });

}