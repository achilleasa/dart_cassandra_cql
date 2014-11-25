part of dart_cassandra_cql.protocol;

class RowsResultMessage extends ResultMessage {
  ResultMetadata metadata;
  List<Map<String, Object>> rows;

  RowsResultMessage.parse(TypeDecoder decoder){

    // Parse metadata
    metadata = _parseMetadata(decoder);

    // Parse rows
    int rowCount = decoder.readUInt();
    rows = new List<Map<String, Object>>.generate(rowCount, (int rowIndex) {
      Map<String, Object> row = new LinkedHashMap();
      metadata.colSpec.forEach((String colName, TypeSpec typeSpec) {
        row[ colName ] = decoder.readTypedValue(typeSpec, size : SizeType.LONG);
      });
      return row;
    });
  }
}