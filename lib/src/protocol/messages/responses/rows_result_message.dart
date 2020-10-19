part of dart_cassandra_cql.protocol;

class RowsResultMessage extends ResultMessage {
  RowsResultMessage.parse(TypeDecoder decoder) {
    // Parse metadata
    metadata = _parseMetadata(decoder);

    // Parse rows
    int rowCount = decoder.readUInt();
    rows = List<Map<String, Object>>.generate(rowCount, (int rowIndex) {
      Map<String, Object> row = LinkedHashMap();
      metadata.colSpec.forEach((String colName, TypeSpec typeSpec) {
        row[colName] = decoder.readTypedValue(typeSpec, size: SizeType.LONG);
      });
      return row;
    });
  }
}
