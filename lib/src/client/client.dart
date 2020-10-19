part of dart_cassandra_cql.client;

class Client {
  final ConnectionPool connectionPool;
  final Map<String, Future<PreparedResultMessage>> preparedQueries =
      Map<String, Future<PreparedResultMessage>>();

  /**
   * Create a new client and a [SimpleConnectionPool] to the supplied [hosts] optionally using
   * the supplied [poolConfig]. If [poolConfig] is not specified, a default configuration will be used instead.
   * If a [defaultKeyspace] is provided, it will be auto selected during the handshake phase of each pool connection
   */

  factory Client.fromHostList(List<String> hosts,
      {String defaultKeyspace, PoolConfiguration poolConfig}) {
    final connectionPool = SimpleConnectionPool.fromHostList(
        hosts, poolConfig == null ? PoolConfiguration() : poolConfig,
        defaultKeyspace: defaultKeyspace);
    return new Client.withPool(connectionPool,
        defaultKeyspace: defaultKeyspace);
  }

  /**
   * Create a new client with an already setup [connectionPool]. If a [defaultKeyspace]
   * is provided, it will be auto-selected during the handshake phase of each pool connection.
   */
  Client.withPool(this.connectionPool, {String defaultKeyspace});

  /**
   * Execute a [Query] or [BatchQuery] and return back a [Future<ResultMessage>]. Depending on
   * the query type the [ResultMessage] will be an instance of [RowsResultMessage], [VoidResultMessage],
   * [SetKeyspaceResultMessage] or [SchemaChangeResultMessage]. The optional [pageSize] and [pagingState]
   * params may be supplied to enable pagination when performing single queries.
   */
  Future<ResultMessage> execute(QueryInterface query,
      {int pageSize: null, Uint8List pagingState: null}) {
    return (query is BatchQuery)
        ? _executeBatch(query)
        : _executeSingle(query as Query,
            pageSize: pageSize, pagingState: pagingState);
  }

  /**
   * Execute a select query and return back a [Iterable] of [Map<String, Object>] with the
   * result rows.
   */
  Future<Iterable<Map<String, Object>>> query(Query query) async {
    // Run query and return back
    return (await _executeSingle(query)).rows;
  }

  /**
   * Lazily execute a select query and return back a [Stream] object which emits one [Map<String, Object]
   * event per result row. The client uses cassandra's pagination API to load additional result pages on
   * demand. The result page size is controlled by the [pageSize] parameter (defaults to 100 rows).
   */
  Stream<Map<String, Object>> stream(Query query, {int pageSize: 100}) {
    return ResultStream(_executeSingle, query, pageSize).stream;
  }

  /**
   * Terminate any opened connections and perform a clean shutdown. If the [drain] flag is set to true,
   * all pool connections will be drained prior to being disconnected and a [Future] will be returned
   * that will complete when all connections are drained. If [drain] is false then the returned [Future]
   * will be already completed.
   */
  Future shutdown(
      {bool drain: true, Duration drainTimeout: const Duration(seconds: 5)}) {
    return connectionPool.disconnect(drain: drain, drainTimeout: drainTimeout);
  }

  /**
   * Prepare the given query and return back a [Future] with a [PreparedResultMessage]
   */
  Future<PreparedResultMessage> _prepare(Query query) {
    // If the query is preparing/already prepared, return its future
    if (preparedQueries.containsKey(query.query)) {
      return preparedQueries[query.query];
    }

    // Queue for preparation and return back a future
    final deferred = connectionPool
        .getConnection()
        .then((Connection conn) => conn.prepare(query));
    preparedQueries[query.query] = deferred;
    return deferred;
  }

  /**
   * Execute a single [query] with optional [pageSize] and [pagingState] data
   * and return back a [Future<ResultMessage>]
   */
  Future<ResultMessage> _executeSingle(Query query,
      {int pageSize: null, Uint8List pagingState: null}) async {
    final completer = Completer<ResultMessage>();

    // If this is a normal query, pick the next available pool connection and execute it
    if (!query.prepared) {
      void _execute() {
        connectionPool
            .getConnection()
            .then((Connection conn) => conn.execute(query,
                pageSize: pageSize, pagingState: pagingState))
            .then(completer.complete)
            // If we lose our connection OR we cannot reserve a connection stream, retry on another connection
            .catchError((_) => _execute(),
                test: (e) =>
                    e is ConnectionLostException ||
                    e is StreamReservationException)
            // Any other error will cause the future to fail
            .catchError(completer.completeError);
      }

      _execute();
      return await completer.future;
    }

    void _prepareAndExecute() {
      // Prepare query; any error will make our returned future fail
      _prepare(query)
          // Fetch a connection for the node this query was prepared at and execute it
          .then((PreparedResultMessage preparedResult) => connectionPool
                  .getConnectionToHost(preparedResult.host, preparedResult.port)
                  .then((Connection conn) => conn.execute(query,
                      preparedResult: preparedResult,
                      pageSize: pageSize,
                      pagingState: pagingState))
                  .then(completer.complete)
                  // If we lose our connection OR we cannot reserve a connection stream, retry on another connection to the same host
                  .catchError((_) => _prepareAndExecute(),
                      test: (e) =>
                          e is ConnectionLostException ||
                          e is StreamReservationException)
                  // We run out of connections to use this prepared result so we need to prepare it again on a new node
                  .catchError((_) {
                preparedQueries.remove(query.query);
                _prepareAndExecute();
              }, test: (e) => e is NoHealthyConnectionsException))
          // Any other error will cause the future to fail
          .catchError(completer.completeError);
    }

    _prepareAndExecute();
    return await completer.future;
  }

  /**
   * Execute a batch [query] and return back a [Future<ResultMessage>]
   */
  Future<ResultMessage> _executeBatch(BatchQuery query) {
    return connectionPool
        .getConnection()
        .then((Connection conn) => conn.executeBatch(query));
  }
}
