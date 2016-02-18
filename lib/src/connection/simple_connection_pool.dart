part of dart_cassandra_cql.connection;

class SimpleConnectionPool extends ConnectionPool {
  // The list of all pool connections
  final List<Connection> _pool = new List<Connection>();

  // The list of maintained connections per host:port combination
  final Map<String, Set<Connection>> _poolPerHost =
      new HashMap<String, Set<Connection>>();

  // Pending list of reconnect attempts
  final Map<String, Future> _pendingReconnects = new HashMap<String, Future>();

  // Server event listeners
  Connection _eventSubscriber;
  StreamSubscription _eventSubscription;

  String defaultKeyspace;

  Completer _poolConnected;

  /**
   * Create the connection pool by spawning [_config.poolSize] connections
   * to the input list of [hosts]
   */

  SimpleConnectionPool.fromHostList(
      List<String> hosts, PoolConfiguration poolConfig,
      {String this.defaultKeyspace}) {
    if (hosts == null || hosts.isEmpty) {
      throw new ArgumentError("Host list cannot be empty");
    }

    if (poolConfig == null) {
      throw new ArgumentError("A valid pool configuration is required");
    }
    this.poolConfig = poolConfig;

    for (int hostIndex = 0; hostIndex < hosts.length; hostIndex++) {
      List<String> hostParts = hosts[hostIndex].split(':');
      int port = hostParts.length > 1
          ? int.parse(hostParts[1])
          : ConnectionPool.DEFAULT_PORT;

      _createPoolForHost(hostParts[0], port);
    }

    // If node auto-discovery is enabled listen for those server events
    if (poolConfig.autoDiscoverNodes) {
      listenForServerEvents([
        EventRegistrationType.STATUS_CHANGE,
        EventRegistrationType.TOPOLOGY_CHANGE
      ]).listen(_handleEventMessage, onError: (_) {});
    }
    //poolLogger.info("Created ${_pool.length} connections to ${hosts.length} hosts");
  }

  /**
   * Establish connections to the pool nodes and return a [Future] to be successfully completed when
   * at least one connection is successfully established. The returned [Future] will fail if an
   * [AuthenticationException] occurs or if all connection attempts fail.
   */
  Future connect() {
    // Already connected/connecting
    if (_poolConnected != null) {
      return _poolConnected.future;
    }

    // Setup a future to be completed when our connections are set up
    _poolConnected = new Completer();
    poolLogger.info("Initializing pool connections");

    //
    int activeConnections = 0;
    int remainingConnections = _pool.length;
    _pool.forEach((Connection conn) {
      conn.open().then((_) {
        // Select the first open connection as our stream listener
        if (_eventSubscriber == null) {
          _registerEventListener(conn);
        }

        activeConnections++;

        // All connection futures resolved. We got at least one connection
        // so we should resolve the _poolConnected future
        if (--remainingConnections == 0) {
          _poolConnected.complete();
        }
      }).catchError((err) {
        poolLogger.severe(err);

        --remainingConnections;

        // If we get an authentication error, report it directly to the client
        if (err is AuthenticationException) {
          _poolConnected.completeError(err);
        }

        // No more connections remaining to be opened
        if (remainingConnections == 0 && !_poolConnected.isCompleted) {
          // All connections failed
          if (activeConnections == 0) {
            _poolConnected.completeError(new NoHealthyConnectionsException(
                "Could not connect to any of the supplied hosts"));
          } else {
            // At least one connection has been established
            _poolConnected.complete();
          }
        }
      });
    });

    return _poolConnected.future;
  }

  /**
   * Disconnect all pool connections. If the [drain] flag is set to true, all pool connections
   * will be drained prior to being disconnected and a [Future] will be returned that will complete
   * when all connections are drained or when the [drainTimeout] expires. If [drain] is false then
   * the returned [Future] will already be completed.
   */
  Future disconnect(
      {bool drain: true, Duration drainTimeout: const Duration(seconds: 5)}) {
    return Future.wait(_pool.map((Connection conn) =>
        conn.close(drain: drain, drainTimeout: drainTimeout)));
  }

  /**
   * Get back an active [Connection] from the pool.
   */
  Future<Connection> getConnection() {
    return _findOneMatchingFilter(
        (Connection conn) => conn.healthy && conn.inService);
  }

  /**
   * Get back an active [Connection] from the pool to the specified [host] and [port].
   */
  Future<Connection> getConnectionToHost(String host, int port) {
    return _findOneMatchingFilter((Connection conn) =>
        conn.healthy &&
        conn.inService &&
        conn.host == host &&
        conn.port == port);
  }

  Future<Connection> _findOneMatchingFilter(Function filter) {
    return connect().then((_) {
      // Prefer healthry connections matching the filter predicate that have available
      // query multiplexing slots so we do not need to wait
      Connection healthyConnection = _pool.firstWhere(
          (Connection conn) => conn.hasAvailableStreams && filter(conn),
          orElse: () => null);

      // Otherwise try to fetch the first healthy connection from the pool matching the filter predicate
      if (healthyConnection == null) {
        healthyConnection = _pool.firstWhere(filter, orElse: () => null);
      }

      if (healthyConnection == null) {
        return new Future.error(new NoHealthyConnectionsException(
            "No healhty connections available"));
      } else {
        // Remove the connection and append it to the end of the list
        // so we can round-robin our connections
        _pool
          ..remove(healthyConnection)
          ..add(healthyConnection);
      }
      return new Future.value(healthyConnection);
    });
  }

  void _registerEventListener(Connection conn) {
    // List for server events and add them to the stream controller
    _eventSubscriber = conn;
    _eventSubscription = conn.listenForEvents([
      EventRegistrationType.STATUS_CHANGE,
      EventRegistrationType.TOPOLOGY_CHANGE,
      EventRegistrationType.SCHEMA_CHANGE
    ]).listen(_eventStreamController.add, onError: (_) {});

    poolLogger.info("Listening for server events on [${conn.connId}]");
  }

  void _handleEventMessage(EventMessage message) {
    poolLogger.fine(
        "Received eventMessage ${EventRegistrationType.nameOf(message.type)}:${EventType.nameOf(message.subType)} ${message.type == EventRegistrationType.SCHEMA_CHANGE ? ("${message.keyspace}" + (!message.changedTable.isEmpty ? ".${message.changedTable}" : "")) : "${message.address}:${message.port}"}");

    switch (message.subType) {
      case EventType.NODE_ADDED:
      case EventType.NODE_UP:
        String hostKey = "${message.address.host}:${message.port}";

        // If we are already processing an UP event for this host, ignore
        if (_pendingReconnects.containsKey(hostKey)) {
          return;
        }

        // If this is a new node setup a host pool for it
        if (!_poolPerHost.containsKey(hostKey)) {
          // If auto-discover is off, ignore this event
          if (!poolConfig.autoDiscoverNodes) {
            return;
          }

          poolLogger.info("Discovered new node [${hostKey}]. Adding to pool");
          _createPoolForHost(message.address.host, message.port);
        } else {
          poolLogger
              .info("Node [${hostKey}] went online. Attempting to reconnect");
        }

        // According to the protocol spec, it might take some time for the node
        // to begin accepting connections so we need to defer our connection attempts.
        _pendingReconnects[hostKey] =
            new Future.delayed(poolConfig.reconnectWaitTime, () {
          _poolPerHost[hostKey].forEach((Connection conn) => conn.open());
          _pendingReconnects.remove(hostKey);
        });

        break;
      case EventType.NODE_REMOVED:
      case EventType.NODE_DOWN:
        String hostKey = "${message.address.host}:${message.port}";
        Set<Connection> hostConnections = _poolPerHost[hostKey];

        // If this is an unknown node, we dont need to do anything
        if (hostConnections == null) {
          return;
        } else if (message.subType == EventType.NODE_REMOVED) {
          poolLogger.info(
              "Node [${hostKey}] removed from cluster. Purging any existing open connections from pool");
          _poolPerHost.remove(hostKey);
          _pendingReconnects.remove(hostKey);
        } else {
          poolLogger.info(
              "Node [${hostKey}] went offline. Closing any existing open connections");
          _pendingReconnects.remove(hostKey);
        }

        // Force-close any existing connections
        hostConnections.forEach((Connection conn) => conn.close(drain: false));

        // If we lost the connection registered for server events, pick a new one
        if (hostConnections.contains(_eventSubscriber)) {
          _eventSubscription.cancel();
          _eventSubscription = null;
          _eventSubscriber = null;
          getConnection().then(_registerEventListener).catchError((_) {});
        }

        break;
    }
  }

  void _createPoolForHost(String host, int port) {
    String hostKey = "${host}:${port}";
    _poolPerHost[hostKey] = new HashSet<Connection>();

    // Allocate poolConfig.connectionsPerHost connections
    for (int poolIndex = 0;
        poolIndex < poolConfig.connectionsPerHost;
        poolIndex++) {
      Connection conn = new Connection("${hostKey}-${poolIndex}", host, port,
          config: poolConfig, defaultKeyspace: defaultKeyspace);

      _poolPerHost[hostKey].add(conn);
      _pool.add(conn);
    }
  }
}
