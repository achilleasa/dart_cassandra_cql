part of dart_cassandra_cql.connection;

class PoolConfiguration {
  // The CQL version to use
  String cqlVersion;

  /**
   * The binary [ProtocolVersion] to use when communicating with cassandra
   */
  ProtocolVersion protocolVersion;

  // The number of concurrent connections for each host in the pool
  int connectionsPerHost;

  // Max requests that can be multiplexed over each connection. According to the protocol spec, each
  // connection can multiplex up to 128 streams in V2 mode and 32768 streams in V3 mode
  int streamsPerConnection;

  // The max number of reconnection attempts before declaring a connection as unusable
  int maxConnectionAttempts;

  // The time to wait  before trying to reconnect
  Duration reconnectWaitTime;

  // Max time we wait to reserve a stream from a connection.
  Duration streamReservationTimeout;

  // If this flag is set to true, the connection pool will listen for server topology change events
  // and automatically update the pool when new nodes come online/go offline. If set to false, the pool
  // will only process UP/DOWN events for nodes already in the pool
  bool autoDiscoverNodes;

  /**
   * Use the following [Compression] algorithm for communicating with cassandra.
   * To use this feature you need to register the appropriate compression [Codec]
   * by invoking [registerCodec]
   */
  Compression compression;

  /**
   * An [Authenticator] instance for answering cassandra AUTH_CHALLENGE messages
   */
  Authenticator authenticator;

  /**
   * Setting the [preferBiggerTcpPackets] option will join together
   * protocol frame data before piping them to the underlying TCP socket.
   * This option will improve performance at the expense of slightly higher memory consumption
   */
  bool preferBiggerTcpPackets;

  PoolConfiguration(
      {String this.cqlVersion: "3.0.0",
      ProtocolVersion this.protocolVersion: ProtocolVersion.V3,
      int this.connectionsPerHost: 1,
      int this.streamsPerConnection: 128,
      int this.maxConnectionAttempts: 10,
      Duration this.reconnectWaitTime: const Duration(milliseconds: 1500),
      Duration this.streamReservationTimeout: const Duration(milliseconds: 0),
      bool this.autoDiscoverNodes: true,
      Compression this.compression,
      Authenticator this.authenticator,
      bool this.preferBiggerTcpPackets: false}) {
    validate();
  }

  /**
   * Validate configuration. Throws [ArgumentError] exception if an invalid value
   * is detected
   */

  void validate() {
    // We only support protocol version V2 and V3
    if (protocolVersion != ProtocolVersion.V2 &&
        protocolVersion != ProtocolVersion.V3) {
      throw ArgumentError("Driver only supports protocol versions 2 and 3");
    }

    // According to the protocol spec, each connection can multiplex up to 128 streams (V2) or 32768 (V3)
    if (protocolVersion == ProtocolVersion.V2 &&
        (streamsPerConnection <= 0 || streamsPerConnection > 128)) {
      throw ArgumentError(
          "Invalid value for option 'streamsPerConnection'. Expected a value between 1 and 128 when using V2 prototcol");
    }
    if (protocolVersion == ProtocolVersion.V3 &&
        (streamsPerConnection <= 0 || streamsPerConnection > 32768)) {
      throw ArgumentError(
          "Invalid value for option 'streamsPerConnection'. Expected a value between 1 and 3768 when using V3 prototcol");
    }

    // If a compression algorithm is specified make sure the appropriate codec is registered
    if (compression != null && getCodec(compression.value) == null) {
      throw ArgumentError(
          "A compression codec needs to be registered via registerCodec() for type '${compression}'");
    }
  }
}
