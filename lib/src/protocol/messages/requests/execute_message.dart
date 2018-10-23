part of dart_cassandra_cql.protocol;

class ExecuteMessage extends QueryMessage implements RequestMessage {
  Uint8List queryId;
  Map<String, TypeSpec> bindingTypes;

  ExecuteMessage() : super() {
    opcode = Opcode.EXECUTE;
  }

  /**
   * Write the bindings for the prepared statement using the
   * binding type data we received when we prepared it as
   * hints to the encoder
   */

  void _writeBindings(TypeEncoder encoder) {
    if (bindings is Map) {
      Map bindingsMap = bindings as Map;
      encoder.writeUInt16(bindingsMap.length);
      bindingsMap.forEach((arg, value) {
        encoder.writeTypedValue(arg, value, typeSpec: bindingTypes[arg]);
      });
    } else {
      Iterable<TypeSpec> bindingTypeList = bindingTypes.values;
      Iterable bindingsList = bindings as Iterable;
      encoder.writeUInt16(bindingsList.length);

      int arg = 0;
      bindingsList.forEach((Object value) {
        encoder.writeTypedValue("$arg", value,
            typeSpec: bindingTypeList.elementAt(arg++));
      });
    }
  }

  void write(TypeEncoder encoder) {
    // Write queryId
    encoder.writeBytes(queryId, SizeType.SHORT);

    // Write query params
    _writeQueryParameters(encoder);
  }
}
