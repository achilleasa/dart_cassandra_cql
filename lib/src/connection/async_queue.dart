part of dart_cassandra_cql.connection;

class AsyncQueue<T> {
  Queue<T> _resources;
  Queue<Completer<T>> _reservations = Queue<Completer<T>>();
  Duration reservationTimeout;

  AsyncQueue.from(Iterable<T> resources) {
    _resources = Queue<T>.from(resources);
  }

  /**
   * Reserve an item of type [T] from the [AsyncQueue]. Returns a [Future<T>]
   * to be completed when an item becomes available
   */

  Future<T> reserve() {
    final reservation = Completer<T>();
    _reservations.add(reservation);
    _dequeue();

    // Set reservation timeout if one is specified
    if (reservationTimeout != null && reservationTimeout.inMilliseconds > 0) {
      Timer(reservationTimeout, () {
        if (!reservation.isCompleted) {
          reservation.completeError(StreamReservationException(
              'Timed out waiting for stream reservation'));
        }
      });
    }

    return reservation.future;
  }

  /**
   * Returns [item] back to the queue
   */

  void release(T resource) {
    _resources.add(resource);
    _dequeue();
  }

  /**
   * Dequeue any available items and notify waiting readers
   */

  void _dequeue() {
    while (_resources.isNotEmpty && _reservations.isNotEmpty) {
      T resource = _resources.removeFirst();
      Completer<T> reservation = _reservations.removeFirst();
      reservation.complete(resource);
    }
  }

  /**
   * Check if the queue has available resources for reservation
   */
  bool get hasAvailableSlots => _resources.isNotEmpty;
}
