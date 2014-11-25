part of dart_cassandra_cql.connection;

abstract class ConnectionPool {

  static final int DEFAULT_PORT = 9042;

  PoolConfiguration poolConfig;
  StreamController<EventMessage> _eventStreamController = new StreamController<EventMessage>.broadcast();

  /**
   * Establish connections to the pool nodes and return a [Future] to be successfully completed when
   * at least one connection is successfully established. The returned [Future] will fail if an
   * [AuthenticationException] occurs or if all connection attempts fail.
   */
  Future connect();

  /**
   * Disconnect all pool connections. If the [drain] flag is set to true, all pool connections
   * will be drained prior to being disconnected and a [Future] will be returned that will complete
   * when all connections are drained. If [drain] is false then the returned [Future] will be already
   * completed.
   */
  Future disconnect({ bool drain : true });

  /**
   * Get back an active [Connection] from the pool.
   */
  Future<Connection> getConnection();

  /**
   * Get back an active [Connection] from the pool to the specified [host] and [port].
   */
  Future<Connection> getConnectionToHost(String host, int port);

  /**
   * Return a [Stream<EventMessage>] where the application can listen for the requested [eventTypes].
   */
  Stream<EventMessage> listenForServerEvents(List<EventRegistrationType> eventTypes) {
    // Return back a stream filtered by the given event types
    return _eventStreamController.stream.where((EventMessage e) => eventTypes.contains(e.type));
  }
}
