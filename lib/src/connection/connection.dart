part of dart_cassandra_cql.connection;

class Connection {
  // Configuration options
  PoolConfiguration _poolConfig;
  String defaultKeyspace;
  String connId;
  String host;
  int port;

  // Flag indicating whether the connected host is healthy or not
  bool healthy = false;

  // Flag indicating whether this connection can process client requests
  bool inService = false;

  // The connection to the server
  Socket _socket;
  int _connectionAttempt;

  // A pool of frame writers for each multiplexed stream
  AsyncQueue<FrameWriter> _frameWriterPool;
  Map<int, FrameWriter> _reservedFrameWriters = new Map<int, FrameWriter>();

  // Tracked futures/streams
  Map<int, Completer<Message>> _pendingResponses;
  Completer _connected;
  Completer _drained;
  Future _socketFlushed;

  Connection(String this.connId, String this.host, int this.port,
      {PoolConfiguration config, String this.defaultKeyspace}) {
    // If no config is specified, use the default one
    _poolConfig = config == null ? new PoolConfiguration() : config;

    // Initialize pending futures list
    _pendingResponses = new Map<int, Completer<Message>>();
  }

  StreamController _eventController;

  /**
   * Abort any pending requests with [reason] as the error and clean up.
   * Returns a [Future] to be completed when the client socket has been successfully closed
   */

  Future _abortRequestsAndCleanup(reason) {
    // Clear healthy flag
    healthy = false;

    // Fail all pending requests and cleanup
    _pendingResponses.values.forEach((Completer completer) {
      if (!completer.isCompleted) {
        completer.completeError(reason);
      }
    });
    _pendingResponses.clear();

    // Cleanup
    _connected = null;

    // Kill socket
    Future socketClosed =
        _socket != null ? _socket.close() : new Future.value();
    _socket = null;

    return socketClosed;
  }

  /**
   * Attempt to reconnect to the server. If the attempt fails, it will be retried after
   * [reconnectWaitTime] ms up to [maxConnectionAttempts] times. If all connection attempts
   * fail, then the [_connected] [Future] returned by a call to [open[ will also fail
   */

  Future _reconnect() {
    if (_connected == null) {
      _connected = new Completer();
    }

    connectionLogger.info(
        "[${connId}] Trying to connect to ${host}:${port} [attempt ${_connectionAttempt + 1}/${_poolConfig.maxConnectionAttempts}]");
    Socket.connect(host, port).then((Socket s) {
      _socket = s;
      _socketFlushed = new Future.value(true);

      // Initialize our writer pool and set the reservation timeout
      _reservedFrameWriters.clear();
      _frameWriterPool = new AsyncQueue<FrameWriter>.from(
          new List<FrameWriter>.generate(
              _poolConfig.streamsPerConnection,
              (int id) => new FrameWriter(id, _poolConfig.protocolVersion,
                  preferBiggerTcpPackets: _poolConfig.preferBiggerTcpPackets)));
      _frameWriterPool.reservationTimeout =
          _poolConfig.streamReservationTimeout;

      // Bind processors and initiate handshake
      _socket
          .transform(new FrameParser().transformer)
          .transform(new FrameDecompressor(_poolConfig.compression).transformer)
          .transform(new FrameReader().transformer)
          .listen(
              _onMessage
              // Mute socket errors; they will be caught by _writeMessage
              ,
              onError: (_) {}, onDone: () {
        connectionLogger.severe("[${connId}] Server has closed the connection");
        if (_socket != null) {
          _socket.destroy();
          _socket = null;
        }

        _abortRequestsAndCleanup(
            new ConnectionLostException("Server closed the connection"));
      });

      // Handshake with the server
      _handshake();
    }).catchError((err, trace) {
      if (++_connectionAttempt >= _poolConfig.maxConnectionAttempts) {
        String errorMessage =
            "[${connId}] Could not connect to ${host}:${port} after ${_poolConfig.maxConnectionAttempts} attempts. Giving up";
        connectionLogger.severe(errorMessage);
        _connected
            .completeError(new ConnectionFailedException(errorMessage, trace));

        // Clear _connected future so the client can invoke open() in the future
        _connected = null;
      } else {
        // Retry after reconnectWaitTime ms
        new Timer(_poolConfig.reconnectWaitTime, _reconnect);
      }
    });

    return _connected.future;
  }

  Future<ResultMessage> _authenticate(AuthenticateMessage authMessage) {
    // Check if an authenticator is specified
    if (_poolConfig.authenticator == null) {
      throw new AuthenticationException(
          "Server requested '${authMessage.authenticatorClass}' authenticator but no authenticator specified");
    } else if (authMessage.authenticatorClass !=
        _poolConfig.authenticator.authenticatorClass) {
      throw new AuthenticationException(
          "Server requested '${authMessage.authenticatorClass}' authenticator but a '${_poolConfig.authenticator.authenticatorClass}' authenticator was specified instead");
    }

    // Run through challenge response till we get back a ready message from the server
    Completer completer = new Completer();

    void answerChallenge(Message result) {
      if (result is AuthenticateMessage || result is AuthChallengeMessage) {
        AuthResponseMessage response = new AuthResponseMessage()
          ..responsePayload = _poolConfig.authenticator.answerChallenge(
              result is AuthenticateMessage
                  ? result.challengePayload
                  : (result as AuthChallengeMessage).challengePayload);

        _writeMessage(response).then(answerChallenge).catchError((e, trace) {
          completer.completeError(
              e is CassandraException
                  ? new AuthenticationException(e.message, trace)
                  : e,
              trace);
        });
      } else if (result is AuthSuccessMessage) {
        completer.complete(result);
      }
    }

    // Begin challenge-response round
    answerChallenge(authMessage);
    return completer.future;
  }

  /**
   * Perform handshake, authenticate with the server and optionally select [defaultKeyspace]
   */

  void _handshake() {
    StartupMessage message = new StartupMessage()
      ..cqlVersion = _poolConfig.cqlVersion
      ..compression = _poolConfig.compression;

    _writeMessage(message).then((Message response) {
      // Authentication Required
      if (response is AuthenticateMessage) {
        return _authenticate(response);
      } else {
        return new Future.value(response);
      }
    }).then((_) {
      // if default keyspace is specified, run a query here.
      // Note since the connection is not yet *open* we cannot invoke execute() here
      if (defaultKeyspace != null) {
        Query query = new Query("USE ${defaultKeyspace}");
        QueryMessage message = new QueryMessage()
          ..query = query.expandedQuery
          ..bindings = null
          ..consistency = query.consistency
          ..serialConsistency = query.serialConsistency;

        return _writeMessage(message);
      }
      return true;
    }).then((_) {
      connectionLogger.info(
          "[${connId}] Connected to ${host}:${port} [attempt ${_connectionAttempt + 1}/${_poolConfig.maxConnectionAttempts}]");

      healthy = true;
      inService = true;
      _drained = null;
      _connected.complete();
    }).catchError((e, trace) {
      healthy = false;
      inService = false;
      _drained = null;
      _connected.completeError(e, trace);
    });
  }

  /**
   * Encode and send a [message] to the server. Returns a [Future] to be
   * completed with the query results or with an error if one occurs
   */

  Future<Message> _writeMessage(RequestMessage message) {
    final reply = new Completer<Message>();
    // Make sure we have flushed our socket data and then
    // block till we get back a frame writer
    // We also assign returned future to _socketFlushed to avoid
    // race conditions on consecutive calls to _writeMessage.
    _socketFlushed = _socketFlushed
        .then((_) => _frameWriterPool.reserve())
        .then((FrameWriter writer) {
      _reservedFrameWriters[writer.getStreamId()] = writer;
      _pendingResponses[writer.getStreamId()] = reply;
      connectionLogger.fine(
          "[${connId}] [stream #${writer.getStreamId()}] Sending message of type ${Opcode.nameOf(message.opcode)} (${message.opcode}) ${message}");
      writer.writeMessage(message, _socket,
          compression: _poolConfig.compression);
      return _socket.flush();
    }).catchError((e, trace) {
      // Wrap SocketExceptions
      if (e is SocketException) {
        _abortRequestsAndCleanup(
            new ConnectionLostException('Lost connection'));
      } else {
        reply.completeError(e);
      }
    });

    return reply.future;
  }

  /**
   * Handle an incoming server [message]
   */

  void _onMessage(Message message) {
    connectionLogger.fine(
        "[${connId}] [stream #${message.streamId}] Received message of type ${Opcode.nameOf(message.opcode)} (${message.opcode}) ${message}");

    // Fetch our response completer
    final responseCompleter = _pendingResponses[message.streamId];

    // Release streamId back to the pool unless its -1 (server event message)
    if (message.streamId != -1) {
      //connectionLogger.fine("[${connId}] Releasing writer for stream #${message.streamId}");

      if (responseCompleter != null) {
        _pendingResponses.remove(message.streamId);
        FrameWriter writer = _reservedFrameWriters.remove(message.streamId);
        _frameWriterPool.release(writer);
      }

      _checkForDrainedRequests();
    }

    // If the frame-grabber caught an exception, report it through the completer
    if (message is ExceptionMessage) {
      if (responseCompleter != null) {
        responseCompleter.completeError(message.exception, message.stackTrace);
      }
      return;
    } else if (message.opcode != Opcode.EVENT && responseCompleter == null) {
      // Connection has probably been aborted before we got to process this message; ignore
      return;
    }

    switch (message.opcode) {
      case Opcode.READY:
        responseCompleter.complete();
        break;
      case Opcode.AUTHENTICATE:
      case Opcode.AUTH_CHALLENGE:
      case Opcode.AUTH_SUCCESS:
        responseCompleter.complete(message);
        break;
      case Opcode.RESULT:
        if (message is PreparedResultMessage) {
          PreparedResultMessage resMsg = message;

          // Fill in our host and port
          resMsg.host = host;
          resMsg.port = port;
        }

        responseCompleter.complete(message);
        break;
      case Opcode.EVENT:
        if (_eventController != null &&
            _eventController.hasListener &&
            !_eventController.isPaused) {
          _eventController.add(message as EventMessage);
        }
        break;
      case Opcode.ERROR:
        ErrorMessage errorMessage = message as ErrorMessage;
        connectionLogger.severe(errorMessage.message);
        responseCompleter
            .completeError(new CassandraException(errorMessage.message));
        break;
    }
  }

  /**
   * Check if we are draining active requests and no more
   * pending requests are available
   */

  void _checkForDrainedRequests() {
    if (_drained != null && _pendingResponses.length == 0) {
      _drained.complete(true);
      _socket.close();
      _socket = null;
      _connected = null;
    }
  }

  /**
   * Open a working connection to the server using [config.cqlVersion] and optionally select
   * keyspace [defaultKeyspace]. Returns a [Future] to be completed on a successful protocol handshake
   */

  Future open() {
    // Prevent multiple connection attempts
    if (_connected != null) {
      return _connected.future;
    }

    _connectionAttempt = 0;
    return _reconnect();
  }

  /**
   * Close the connection and set the [inService] flag to false so no new
   * requests are sent to this connection. If the [drain] flag is set then the socket
   * will be disconnected when all pending requests finish or when [drainTimeout] expires.
   * Otherwise, the socket will be immediately disconnected and any pending requests will
   * automatically fail.
   *
   * Once the connection is closed, any further invocations of close() will immediately
   * succeed.
   *
   * This method returns a [Future] to be completed when the connection is shut down
   */

  Future close(
      {bool drain: true, Duration drainTimeout: const Duration(seconds: 5)}) {
    // Already closed
    if (_socket == null) {
      return new Future.value();
    }

    connectionLogger.info("[${connId}] Closing (drain = ${drain})");

    // Prevent clients from submitting new requests to us
    inService = false;

    if (drain) {
      inService = false;
      if (_drained == null) {
        _drained = new Completer();
        _checkForDrainedRequests();

        if (drainTimeout != null) {
          new Future.delayed(drainTimeout).then((_) {
            if (_drained != null && !_drained.isCompleted) {
              _abortRequestsAndCleanup(
                      new ConnectionLostException('Connection drain timeout'))
                  .then((_) {
                _drained.complete();
              });
            }
          });
        }
      }

      return _drained.future;
    } else {
      return _abortRequestsAndCleanup(
          new ConnectionLostException('Connection closed'));
    }
  }

  Future<PreparedResultMessage> prepare(Query query) {
    return open().then((_) async {
      // V2 version of the protocol does not support named placeholders. We need to convert them
      // to positional ones before preparing the statements
      PrepareMessage message = new PrepareMessage()
        ..query = _poolConfig.protocolVersion == ProtocolVersion.V2
            ? query.positionalQuery
            : query.query;

      return (await _writeMessage(message)) as PreparedResultMessage;
    });
  }

  /**
   * Execute a single prepared or unprepared [query]. In the case of a prepared query,
   * the optional [preparedResult] argument needs to be specified. To page through
   * queries, the [pageSize] and [pagingState] fields should be supplied for the initial and
   * each consecutive invocation.
   *
   * This method will returns [ResultMessage] with the query result
   */

  Future<ResultMessage> execute(Query query,
      {PreparedResultMessage preparedResult: null,
      int pageSize: null,
      Uint8List pagingState: null}) {
    return open().then((_) async {
      // Simple unprepared query
      if (preparedResult == null) {
        QueryMessage message = new QueryMessage()
          ..query = query.expandedQuery
          ..bindings = null
          ..consistency = query.consistency
          ..serialConsistency = query.serialConsistency
          ..resultPageSize = pageSize
          ..pagingState = pagingState;

        return await _writeMessage(message);
      } else {
        // Prepared query. V2 of the protocol does not support named bindings so we need to
        // map them to positional ones
        ExecuteMessage message = new ExecuteMessage()
          ..queryId = preparedResult.queryId
          ..bindings = _poolConfig.protocolVersion == ProtocolVersion.V2
              ? query.namedToPositionalBindings
              : query.bindings
          ..bindingTypes = preparedResult.metadata.colSpec
          ..consistency = query.consistency
          ..serialConsistency = query.serialConsistency
          ..resultPageSize = pageSize
          ..pagingState = pagingState;

        return await _writeMessage(message);
      }
    });
  }

  /**
   * Execute the supplied batch [query]
   */

  Future<ResultMessage> executeBatch(BatchQuery query) {
    return open().then((_) async {
      BatchMessage message = new BatchMessage()
        ..type = query.type
        ..consistency = query.consistency
        ..serialConsistency = query.serialConsistency
        ..queryList = query.queryList;

      return await _writeMessage(message);
    });
  }

  /**
   * Request server notifications for each [EventRegistrationType] in [eventTypes]
   * and return a [Stream<EventMessage>] for handling incoming events
   */

  Stream<EventMessage> listenForEvents(
      Iterable<EventRegistrationType> eventTypes) {
    RegisterMessage message = new RegisterMessage()..eventTypes = eventTypes;

    // Setup the stream controller
    if (_eventController == null) {
      _eventController = new StreamController<EventMessage>();
    }

    open()
        .then((_) => _writeMessage(message))
        .catchError(_eventController.addError);
    return _eventController.stream;
  }

  /**
   * Check if the connection has available stream slots for multiplexing additional queries
   */
  bool get hasAvailableStreams =>
      _frameWriterPool != null && _frameWriterPool.hasAvailableSlots;
}
