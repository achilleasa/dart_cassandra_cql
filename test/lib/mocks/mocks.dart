library dart_cassandra_cql.tests.mocks;

import "dart:typed_data";
import "dart:io";
import "dart:async";
import "package:logging/logging.dart";
import '../../../lib/src/stream.dart';
import '../../../lib/src/protocol.dart';
import '../../../lib/src/types.dart';

final Logger mockLogger = new Logger("MockLogger");
bool initializedLogger = false;

void initLogger() {
  if( initializedLogger == true ){
    return;
  }
  initializedLogger = true;
  hierarchicalLoggingEnabled = true;
  Logger.root.level = Level.ALL;
  Logger.root.onRecord.listen((LogRecord rec) {
    print("[${rec.level.name}]\t[${rec.time}]\t[${rec.loggerName}]:\t${rec.message}");
  });
}

void writeMessage(Sink targetSink, int opcode, {
ProtocolVersion protocolVersion : ProtocolVersion.V2
, HeaderVersion headerVersion : HeaderVersion.REQUEST_V2
, int streamId : 0
, int flags : 0
, int overrideLength
, List<int> data : const []
}) {
  TypeEncoder typeEncoder = new TypeEncoder(protocolVersion);

  // Allocate header buffer
  Uint8List buf = new Uint8List(protocolVersion == ProtocolVersion.V2
                                ? FrameHeader.SIZE_IN_BYTES_V2
                                : FrameHeader.SIZE_IN_BYTES_V3);
  ByteData headerBytes = new ByteData.view(buf.buffer);

  // Encode header
  int offset = 0;
  headerBytes
    ..setUint8(offset++, headerVersion.value)
    ..setUint8(offset++, flags);

  // Encode stream id (V2 uses a byte, V3 uses a short)
  if (protocolVersion == ProtocolVersion.V2) {
    headerBytes.setInt8(offset++, streamId);
  } else {
    headerBytes.setInt16(offset, streamId);
    offset += 2;
  }

  // Encode remaining frame data
  headerBytes
    ..setUint8(offset++, opcode)
    ..setUint32(offset++, overrideLength != null
                          ? overrideLength
                          : data.length);

  // Prepend the header to the writer buffer queue
  typeEncoder.writer.addFirst(buf);
  typeEncoder.writer.addLast(new Uint8List.fromList(data));

  // Pipe everything to the sink
  typeEncoder.writer.pipe(targetSink);
}

TypeDecoder createDecoder(TypeEncoder fromEncoder) {
  // Pipe encoded data to a reader
  ChunkedInputReader reader = new ChunkedInputReader();
  fromEncoder.writer.chunks.forEach(reader.add);

  // Read to a buffer
  Uint8List buffer = new Uint8List(reader.length);
  reader.read(buffer, reader.length);

  // Return decoder
  return new TypeDecoder.fromBuffer(
      new ByteData.view(buffer.buffer),
      fromEncoder.protocolVersion
  );
}

class MockServer {

  Compression _compression;
  ServerSocket _server;
  List<Socket> clients = [];
  List<String> _replayDumpFileList;
  List<String> _replayAuthDumpFileList;
  String _pathToDumps;
  Duration responseDelay;

  MockServer() {
    List<String> pathSegments = Platform.script.pathSegments.getRange(0, Platform.script.pathSegments.length - 1).toList();
    // Hack: make sure we can find our dump files from the main test or from individual tests
    if (!pathSegments.contains("lib")) {
      pathSegments = new List.from(pathSegments);
      pathSegments.add("lib");
    }
    _pathToDumps = "${Platform.pathSeparator}${pathSegments.join(Platform.pathSeparator)}${Platform.pathSeparator}frame_dumps${Platform.pathSeparator}";
  }

  Future shutdown() {
    _replayDumpFileList = null;
    _replayAuthDumpFileList = null;

    if (_server != null) {
      mockLogger.info("Shutting down server [${_server.address}:${_server.port}]");

      List<Future> cleanupFutures = []
        ..addAll(clients.map((Socket client) => new Future.value(client.destroy())))
        ..add(_server.close().then((_) => new Future.delayed(new Duration(milliseconds:20), () => true)));

      clients.clear();
      _server = null;

      return Future.wait(cleanupFutures);
    }

    return new Future.value();
  }

  void disconnectClient(int clientIndex) {
    if (clients.length > clientIndex) {
      Socket client = clients.removeAt(clientIndex);
      mockLogger.info("Disconnecting client [${client.remoteAddress.host}:${client.remotePort}]");
      client.destroy();
    }
  }

  Uint8List _applyCompression(List<int> originalPayload) {
    //mockLogger.fine("Applying '${_compression}' compression to frame of size ${originalPayload.length}");
    Uint8List payload = new Uint8List.fromList(originalPayload);

    // Detect version
    ProtocolVersion version = (
                                  payload[0] == HeaderVersion.REQUEST_V2.value ||
                                  payload[0] == HeaderVersion.RESPONSE_V2.value
                              )
                              ? ProtocolVersion.V2
                              : ProtocolVersion.V3;

    // Create encoder for assembling compressed output
    TypeEncoder encoder = new TypeEncoder(version);

    // Create header and body views
    ByteData headerView = new ByteData.view(payload.buffer, 0, version == ProtocolVersion.V2 ? FrameHeader.SIZE_IN_BYTES_V2 : FrameHeader.SIZE_IN_BYTES_V3);
    Uint8List bodyView = new Uint8List.view(payload.buffer, version == ProtocolVersion.V2 ? FrameHeader.SIZE_IN_BYTES_V2 : FrameHeader.SIZE_IN_BYTES_V3, payload.lengthInBytes - headerView.lengthInBytes);

    // Compress body
    int originalLen = bodyView.lengthInBytes;
    Uint8List compressedBody = getCodec(_compression.value).encode(bodyView);

    // Assemble compressed payload:
    encoder.writer.addLast(compressedBody);

    // Toggle header compression flag and update body size
    headerView.setUint8(1, headerView.getUint8(1) | HeaderFlag.COMPRESSION.value);
    headerView.setUint32(version == ProtocolVersion.V2 ? 4 : 5, encoder.writer.lengthInBytes);

    // Prepend header to writer blocks
    encoder.writer.addFirst(new Uint8List.view(headerView.buffer, 0, headerView.lengthInBytes));

    // concat everything together and cleanup
    Uint8List compressedOutput = encoder.writer.joinChunks();
    encoder.writer.clear();
    return compressedOutput;
  }

  Uint8List _patchStreamId(List<int> originalPayload, int streamId) {
    if (streamId == null) {
      return originalPayload;
    }
    Uint8List payload = new Uint8List.fromList(originalPayload);

    // Detect version
    ProtocolVersion version = (
                                  payload[0] == HeaderVersion.REQUEST_V2.value ||
                                  payload[0] == HeaderVersion.RESPONSE_V2.value
                              )
                              ? ProtocolVersion.V2
                              : ProtocolVersion.V3;

    // Path stream id
    ByteData headerView = new ByteData.view(payload.buffer, 0, version == ProtocolVersion.V2 ? FrameHeader.SIZE_IN_BYTES_V2 : FrameHeader.SIZE_IN_BYTES_V3);

    if (version == ProtocolVersion.V2) {
      headerView.setInt8(2, streamId);
    } else {
      headerView.setInt16(2, streamId);
    }

    return payload;
  }

  Future replayFile(int clientIndex, String filename, [int streamId = null]) {
    Future onReplay() {
      if (clientIndex > clients.length - 1) {
        throw new ArgumentError("Invalid client index");
      }
      File dumpFile = new File("${_pathToDumps}${filename}");
      List<int> response = _compression == null
                           ? _patchStreamId(dumpFile.readAsBytesSync(), streamId)
                           : _applyCompression(_patchStreamId(dumpFile.readAsBytesSync(), streamId));
      clients[clientIndex].add(response);
      return clients[clientIndex].flush();

    }

    return responseDelay != null
           ? new Future.delayed(responseDelay, onReplay)
           : onReplay();
  }

  Future listen(String host, int port) {
    Completer completer = new Completer();
    mockLogger.info("Binding MockServer to $host:$port");

    ServerSocket.bind(host, port).then((ServerSocket server) {
      _server = server;
      mockLogger.info("[$host:$port] Listening for incoming connections");
      _server.listen(_handleConnection);
      completer.complete();
    });

    return completer.future;

  }

  void setCompression(Compression compressionAlgo) {
    this._compression = compressionAlgo;
  }

  void setReplayList(Iterable<String> list) {

    _replayDumpFileList = new List.from(list);
  }

  void setAuthReplayList(Iterable<String> list) {

    _replayAuthDumpFileList = new List.from(list);
  }

  void _handleClientFrame(Socket client, Frame frame) {
    //mockLogger.fine("Client [${client.remoteAddress.host}:${client.remotePort}][SID: ${frame.header.streamId}] sent ${Opcode.nameOf(frame.header.opcode)} frame with len 0x${frame.header.length.toRadixString(16)}]");
    // Complete handshake and event registration messages
    if (frame.header.opcode == Opcode.STARTUP && (_replayAuthDumpFileList == null || _replayAuthDumpFileList.isEmpty)) {
      writeMessage(client, Opcode.READY.value, streamId : frame.header.streamId);
    } else if (frame.header.opcode == Opcode.REGISTER) {
      writeMessage(client, Opcode.READY.value, streamId : frame.header.streamId);
    } else if (_replayAuthDumpFileList != null && !_replayAuthDumpFileList.isEmpty) {
      // Respond with the next payload in replay list
      replayFile(clients.indexOf(client), _replayAuthDumpFileList.removeAt(0), frame.header.streamId);
    } else if (_replayDumpFileList != null && !_replayDumpFileList.isEmpty) {
      // Respond with the next payload in replay list
      replayFile(clients.indexOf(client), _replayDumpFileList.removeAt(0), frame.header.streamId);
    }
  }

  void _handleClientError(Socket client, err, trace) {
    mockLogger.info("Client [${client.remoteAddress.host}:${client.remotePort}] error ${err.exception.message}");
    mockLogger.info("${err.stackTrace}");
  }

  void _handleConnection(Socket client) {
    clients.add(client);
    mockLogger.info("Client [${client.remoteAddress.host}:${client.remotePort}] connected");

    client
    .transform(new FrameParser().transformer)
    .transform(new FrameDecompressor(_compression).transformer)
    .listen(
            (frame) => _handleClientFrame(client, frame)
        , onError : (err, trace) => _handleClientError(client, err, trace)
    );
  }
}

class MockChunkedOutputWriter extends ChunkedOutputWriter {

  int _forcedLengthInBytes = 0;

  set forcedLengthInBytes(int value) => _forcedLengthInBytes = value;

  int get lengthInBytes => _forcedLengthInBytes;
}

class MockAuthenticator extends Authenticator {

  String get authenticatorClass {
    return "com.achilleasa.FooAuthenticator";
  }

  Uint8List answerChallenge(Uint8List challenge) {
    return null;
  }

}