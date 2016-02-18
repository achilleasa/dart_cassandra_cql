part of dart_cassandra_cql.query;

class BatchQuery extends QueryInterface {
  List<Query> queryList = new List<Query>();

  Consistency consistency;
  Consistency serialConsistency;
  BatchType type;

  BatchQuery(
      {Consistency this.consistency: Consistency.QUORUM,
      Consistency this.serialConsistency,
      BatchType this.type: BatchType.LOGGED});

  /**
   * Add a new [Query] to the batch
   */

  void add(Query query) {
    queryList.add(query);
  }
}
