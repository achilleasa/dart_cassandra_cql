part of dart_cassandra_cql.types;

class TypeSpec {
  DataType valueType;
  TypeSpec keySubType;
  TypeSpec valueSubType;

  // Custom type
  String customTypeClass;

  // V3 protocol: UDT
  String keyspace;
  String udtName;
  Map<String, TypeSpec> udtFields;

  // V3 protocol: TUPLE
  List<TypeSpec> tupleFields;

  TypeSpec(DataType this.valueType,
      {TypeSpec this.keySubType, TypeSpec this.valueSubType}) {
    if (valueType == DataType.LIST &&
        (valueSubType == null || valueSubType is! TypeSpec)) {
      throw ArgumentError(
          "LIST type should specify a TypeSpec instance for its values");
    } else if (valueType == DataType.SET &&
        (valueSubType == null || valueSubType is! TypeSpec)) {
      throw ArgumentError(
          "SET type should specify a TypeSpec instance for its values");
    } else if (valueType == DataType.MAP &&
        (keySubType == null ||
            keySubType is! TypeSpec ||
            valueSubType == null ||
            valueSubType is! TypeSpec)) {
      throw ArgumentError(
          "MAP type should specify TypeSpec instances for both its keys and values");
    } else if (valueType == DataType.UDT) {
      udtFields = LinkedHashMap<String, TypeSpec>();
    } else if (valueType == DataType.TUPLE) {
      tupleFields = List<TypeSpec>();
    }
  }

  String toString() {
    if (valueType == null) {
      return "<NULL>";
    }
    switch (valueType) {
      case DataType.CUSTOM:
        return "CustomType<${customTypeClass}>";
      case DataType.MAP:
        return "Map<${keySubType}, ${valueSubType}>";
      case DataType.LIST:
        return "List<${valueSubType}>";
      case DataType.SET:
        return "Set<${valueSubType}>";
      case DataType.UDT:
        return "{${keyspace}.${udtName}: ${udtFields}}";
      case DataType.TUPLE:
        return "(${tupleFields})";
      default:
        return DataType.nameOf(valueType);
    }
  }
}
