part of dart_cassandra_cql.client;

typedef Future PagedQueryExecutor(Query query, {int pageSize, Uint8List pagingState});

class ResultStream {
  PagedQueryExecutor _queryExecutor;
  StreamController<Map<String, Object>> _streamController;
  Uint8List _pagingState;
  Queue<Map<String, Object>> _bufferedData;
  Query _query;
  int _pageSize;
  bool _buffering = false;

  void _bufferNextPage() {
    if (_buffering) {
      return;
    }
    _buffering = true;

    _queryExecutor(
        _query
        , pageSize : _pageSize
        , pagingState : _pagingState
    ).then((RowsResultMessage data) {
      // If the stream has been closed, clean up
      if (_streamController.isClosed) {
        return;
      }

      _buffering = false;

      // Append incoming rows to current result list and update our paging state
      _bufferedData = new Queue.from(data.rows);
      data.rows = null;
      _pagingState = data.metadata.pagingState;

      _emitRows();
    })
    .catchError(_streamController.addError, test : (e) => e is NoHealthyConnectionsException)
    .catchError((_) {
      // Treat any other kind of error as a 'connection lost' event and try to rebuffer again
      _buffering = false;
      _bufferNextPage();
    });
  }

  void _emitRows() {

    // If stream is paused, do not emit any events
    if (_streamController.isPaused) {
      return;
    }

    // Emit each available row
    while (_bufferedData != null && _bufferedData.isNotEmpty) {

      Map<String, Object> row = _bufferedData.removeFirst();
      _streamController.add(row);

      // if after adding the row, we detect that the stream is paused or closed, stop streaming
      if (_streamController.isClosed || _streamController.isPaused) {
        break;
      }
    }

    // If our stream is active and we emitted all page rows, fetch the next row
    // or close the stream if we are done
    if (!_streamController.isClosed &&
        !_streamController.isPaused &&
        _bufferedData.isEmpty
    ) {

      if (_pagingState == null) {
        _streamController.close();
      } else {
        _bufferNextPage();
      }
    }
  }

  void _cleanup() {
    _bufferedData = null;
    _pagingState = null;

  }

  Stream<Map<String, Object>> get stream => _streamController.stream;

  /**
   * Create a new [ResultStream] by paging through [this._query] object with a page size of [this._pageSize].
   */
  ResultStream(PagedQueryExecutor this._queryExecutor, Query this._query, int this._pageSize) {
    _streamController = new StreamController<Map<String, Object>>(
        onListen : _bufferNextPage
        , onResume : _emitRows
        , onCancel : _cleanup
        , sync : true
    );
  }
}
