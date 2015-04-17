part of dart_cassandra_cql.stream;

class ChunkedOutputWriter {

  final ListQueue<Uint8List> _bufferedChunks = new ListQueue<Uint8List>();

  /**
   * Add a [chunk] to the head of the buffer queue
   */

  void addFirst(Uint8List chunk) => _bufferedChunks.addFirst(chunk);

  /**
   * Append a [chunk] to the buffer queue
   */

  void addLast(Uint8List chunk) => _bufferedChunks.add(chunk);

  /**
   * Append an [iterable] of chunks to our chunks
   */

  void addAll(Iterable<Uint8List> iterable) => _bufferedChunks.addAll(iterable);

  /**
   * Clear buffer
   */

  void clear() => _bufferedChunks.clear();

  /**
   * Get the total available bytes in all chunk buffers (excluding bytes already de-queued from head buffer).
   */

  int get lengthInBytes => _bufferedChunks.fold(
      0, (int count, el) => count + el.length
  );

  /**
   * Get back the [ListQueue<Uint8List>] of written chunks
   */

  ListQueue<Uint8List> get chunks => _bufferedChunks;

  /**
   * Pipe all buffered chunks to [destination] and clear the buffer queue
   * [preferBiggerTcpPackets] may be set to true to pre-join the chunks and pipe them
   * as a contiguous chunk. This reduces the number of transmitted TCP packets
   * and should improve performance at the expense of a slightly higher memory usage
   */

  void pipe(Sink destination, {bool preferBiggerTcpPackets : false}) {
    if (destination == null) {
      return;
    }
    if( preferBiggerTcpPackets ){
      destination.add(joinChunks());
    } else {
      _bufferedChunks.forEach((Uint8List block) => destination.add(block));
    }
    clear();
  }

  /**
   * Join all chunk blocks into a contiguous chunk
   */
  Uint8List joinChunks() {
    Uint8List out = new Uint8List(lengthInBytes);
    int offset = 0;
    _bufferedChunks.forEach((Uint8List block) {
      int len = block.lengthInBytes;
      out.setRange(offset, offset + len, block);
      offset += len;
    });

    return out;
  }
}
