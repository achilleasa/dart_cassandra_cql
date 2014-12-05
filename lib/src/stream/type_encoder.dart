part of dart_cassandra_cql.stream;

class SizeType extends Enum<int> {

  static const SizeType BYTE = const SizeType._(1);
  static const SizeType SHORT = const SizeType._(2);
  static const SizeType LONG = const SizeType._(4);

  const SizeType._(int value) : super(value);
}

int DECIMAL_FRACTION_DIGITS = 6;

class TypeEncoder {

  // Cassandra spec specifies NULL as the short int value -1
  static const int CASSANDRA_NULL = -1;

  ChunkedOutputWriter _writer;

  Endianness endianess = Endianness.BIG_ENDIAN;

  ProtocolVersion protocolVersion;

  TypeEncoder(ProtocolVersion this.protocolVersion, {ChunkedOutputWriter withWriter : null}) {
    _writer = withWriter == null
              ? new ChunkedOutputWriter()
              : withWriter;
  }

  void writeUint8(int value) {
    Uint8List buf = new Uint8List(1);
    new ByteData.view(buf.buffer).setUint8(0, value);
    _writer.addLast(buf);
  }

  void writeUInt16(int value) {
    Uint8List buf = new Uint8List(2);
    new ByteData.view(buf.buffer).setUint16(0, value, endianess);
    _writer.addLast(buf);
  }

  void writeInt16(int value) {
    Uint8List buf = new Uint8List(2);
    new ByteData.view(buf.buffer).setInt16(0, value, endianess);
    _writer.addLast(buf);
  }

  void writeUInt32(int value) {
    Uint8List buf = new Uint8List(4);
    new ByteData.view(buf.buffer).setUint32(0, value, endianess);
    _writer.addLast(buf);
  }

  void writeInt32(int value) {
    Uint8List buf = new Uint8List(4);
    new ByteData.view(buf.buffer).setInt32(0, value, endianess);
    _writer.addLast(buf);
  }

  void writeUInt64(int value) {
    Uint8List buf = new Uint8List(8);
    new ByteData.view(buf.buffer).setUint64(0, value, endianess);
    _writer.addLast(buf);
  }

  writeFloat(double value) {
    Uint8List buf = new Uint8List(4);
    new ByteData.view(buf.buffer).setFloat32(0, value, endianess);
    _writer.addLast(buf);
  }

  writeDouble(double value) {
    Uint8List buf = new Uint8List(8);
    new ByteData.view(buf.buffer).setFloat64(0, value, endianess);
    _writer.addLast(buf);
  }

  void writeLength(int len, SizeType size) {
    if (size == SizeType.SHORT) {
      writeInt16(len);
    } else {
      writeInt32(len);
    }
  }

  void writeNull(SizeType size) {
    if (size == SizeType.SHORT) {
      writeInt16(CASSANDRA_NULL);
    } else {
      writeInt32(CASSANDRA_NULL);
    }
  }

  void writeBytes(Uint8List value, SizeType size) {
    if (value == null) {
      writeNull(size);
      return;
    }

    // Write the length followed by the actual bytes
    writeLength(value.length, size);
    _writer.addLast(value);
  }

  void writeString(String value, SizeType size) {
    if (value == null) {
      writeNull(size);
      return;
    }

    // Convert to UTF-8
    List<int> bytes = UTF8.encode(value);

    // Write the length followed by the actual bytes
    writeLength(bytes.length, size);
    _writer.addLast(bytes);
  }

  void writeStringList(List value, SizeType size) {
    if (value == null) {
      writeNull(size);
      return;
    }

    // Write the length followed by a string for each K,V
    writeLength(value.length, size);
    value.forEach((Object v) {
      writeString(v.toString(), size);
    });
  }

  void writeStringMap(Map<String, String> value, SizeType size) {
    if (value == null) {
      writeNull(size);
      return;
    }

    // Write the length followed by a string for each K,V
    writeLength(value.length, size);
    value.forEach((String k, String v) {
      writeString(k, size);
      writeString(v, size);
    });
  }

  void writeStringMultiMap(Map<String, List<String>> value, SizeType size) {
    if (value == null) {
      writeNull(size);
      return;
    }

    // Write the length followed by a string, stringlist tuple for each K,V
    writeLength(value.length, size);
    value.forEach((String k, List<String> v) {
      writeString(k, size);
      writeStringList(v, size);
    });
  }

  void _writeAsciiString(String value, SizeType size) {
    if (value == null) {
      writeNull(size);
      return;
    }

    Uint8List bytes = new Uint8List.fromList(ASCII.encode(value));

    // Write the length followed by the actual bytes
    writeLength(value.length, size);
    _writer.addLast(bytes);
  }

  void _writeUUID(Uuid uuid, SizeType size) {
    if (uuid == null) {
      writeNull(size);
      return;
    }

    writeBytes(uuid.bytes, size);
  }

  void _writeVarInt(int value, SizeType size) {
    List<int> bytes = [];
    for (int bits = value.bitLength; bits > 0; bits -= 8, value >>= 8) {
      bytes.add(value & 0xFF);
    }
    if (value < 0) {
      bytes.add(0xFF);
    }
    writeBytes(new Uint8List.fromList(bytes.reversed.toList()), size);
  }

  void _writeDecimal(num value, SizeType size) {
    List<int> bytes = [];

    int scale = value is int ? 0 : DECIMAL_FRACTION_DIGITS;
    int scaledValue = (value * pow(10, scale)).round();

    // Encode scaled value
    for (int bits = scaledValue.bitLength; bits > 0; bits -= 8, scaledValue >>= 8) {
      bytes.add(scaledValue & 0xFF);
    }

    // Encode scale as an int
    bytes.add(scale & 0xFF);
    bytes.add((scale >> 8) & 0xFF);
    bytes.add((scale >> 16) & 0xFF);
    bytes.add((scale >> 24) & 0xFF);

    writeBytes(new Uint8List.fromList(bytes.reversed.toList()), size);
  }

  void writeTypedValue(String name, Object value, {TypeSpec typeSpec : null, DataType forceType : null, SizeType size: SizeType.LONG}) {

    DataType valueType = typeSpec != null ? typeSpec.valueType : forceType;
    //_logger.fine("[TypeEncoder::writeTypedValue] Attempting to write ${DataType.nameOf(valueType)} @ 0x${(encoder.writer.lengthInBytes + (encoder.protocolVersion == ProtocolVersion.V2 ? Header.SIZE_IN_BYTES_V2 : Header.SIZE_IN_BYTES_V3)).toRadixString(16)}");

    if (value == null) {
      writeNull(size);
      return;
    }

    switch (valueType) {
      case DataType.ASCII:
        _writeAsciiString(value, size);
        break;
      case DataType.TEXT:
      case DataType.VARCHAR:
        writeString(value, size);
        break;
      case DataType.UUID:
      case DataType.TIMEUUID:
        if (value is! Uuid) {
          throw new ArgumentError("Expected value for field '${name}' of type ${DataType.nameOf(valueType)} to be an instance of Uuid");
        }
        _writeUUID(value, size);
        break;
      case DataType.CUSTOM:
      // If this is a Uint8List write is to the byte stream.
      // Otherwise, check if this is a CustomType instance with a registered codec
        if (value is Uint8List) {
          writeBytes(value, size);
        } else if (value is CustomType) {
          Codec<Object, Uint8List> codec = getCodec(value.customTypeClass);
          if (codec != null) {
            writeBytes(codec.encode(value), size);
          } else {
            throw new ArgumentError("No custom type handler codec registered for custom type: ${value.customTypeClass}");
          }
        } else {
          throw new ArgumentError("Expected value for field '${name}' of type ${DataType.nameOf(valueType)} to be an instance of Uint8List OR an instance of CustomType with a registered type handler");
        }
        break;
      case DataType.BLOB:
        if (value is! Uint8List) {
          throw new ArgumentError("Expected value for field '${name}' of type ${DataType.nameOf(valueType)} to be an instance of Uint8List");
        }
        writeBytes(value as Uint8List, size);
        break;
      case DataType.INT:
        writeLength(4, size);
        writeInt32(value);
        break;
      case DataType.BIGINT:
      case DataType.COUNTER:
        writeLength(8, size);
        writeUInt64(value);
        break;
      case DataType.TIMESTAMP:
        if (value is! DateTime) {
          throw new ArgumentError("Expected value for field '${name}' of type ${DataType.nameOf(valueType)} to be an instance of DateTime");
        }
        writeLength(8, size);
        writeUInt64((value as DateTime).millisecondsSinceEpoch);
        break;
      case DataType.BOOLEAN:
        writeLength(1, size);
        writeUint8(value == true ? 0x01 : 0x00);
        break;
      case DataType.FLOAT:
        writeLength(4, size);
        writeFloat(value);
        break;
      case DataType.DOUBLE:
        writeLength(8, size);
        writeDouble(value);
        break;
      case DataType.INET:
        if (value is! InternetAddress) {
          throw new ArgumentError("Expected value for field '${name}' of type ${DataType.nameOf(valueType)} to be an instance of InternetAddress");
        }
        writeBytes((value as InternetAddress).rawAddress, size);
        break;
      case DataType.LIST:
      case DataType.SET:
        if (value is! Iterable) {
          throw new ArgumentError("Expected value for field '${name}' of type ${DataType.nameOf(valueType)} to implement Iterable");
        }
        Iterable v = value as Iterable;

        // Encode list length and items into a separate buffer, then write the buffer length and buffer data
        SizeType itemSize = protocolVersion == ProtocolVersion.V2
                            ? SizeType.SHORT
                            : SizeType.LONG;
        TypeEncoder scopedEncoder = new TypeEncoder(protocolVersion);
        scopedEncoder.writeLength(v.length, itemSize);
        v.forEach((Object elem) => scopedEncoder.writeTypedValue(name, elem, typeSpec : typeSpec.valueSubType, size: itemSize));

        // Write buffer size in bytes and the actual buffer data
        writeLength(scopedEncoder.writer.lengthInBytes, size);
        writer.addAll(scopedEncoder.writer.chunks);
        break;
      case DataType.MAP:
        if (value is! Map) {
          throw new ArgumentError("Expected value for field '${name}' of type ${DataType.nameOf(valueType)} to implement Map");
        }
        Map v = value as Map;

        // Encode list items into a separate buffer, then write the buffer length and buffer data
        SizeType itemSize = protocolVersion == ProtocolVersion.V2
                            ? SizeType.SHORT
                            : SizeType.LONG;
        TypeEncoder scopedEncoder = new TypeEncoder(protocolVersion);
        scopedEncoder.writeLength(v.length, itemSize);
        v.forEach((Object key, Object val) {
          scopedEncoder
            ..writeTypedValue(name, key, typeSpec : typeSpec.keySubType, size: itemSize)
            ..writeTypedValue(name, val, typeSpec : typeSpec.valueSubType, size: itemSize);
        });

        // Write buffer size in bytes and the actual buffer data
        writeLength(scopedEncoder.writer.lengthInBytes, size);
        writer.addAll(scopedEncoder.writer.chunks);

        break;
      case DataType.DECIMAL:
        _writeDecimal(value, size);
        break;
      case DataType.VARINT:
        _writeVarInt(value, size);
        break;
      case DataType.UDT:
        if (value is! Map) {
          throw new ArgumentError("Expected value for field '${name}' of type ${DataType.nameOf(valueType)} to implement Map");
        }
        Map v = value as Map;

        // Encode items into a separate buffer, then write the buffer length and buffer data
        SizeType itemSize = SizeType.LONG;
        TypeEncoder scopedEncoder = new TypeEncoder(protocolVersion);
        typeSpec.udtFields.forEach((String name, TypeSpec udtType) {
          scopedEncoder.writeTypedValue(name, v[name], typeSpec: udtType, size: itemSize);
        });

        // Write buffer size in bytes and the actual buffer data
        writeLength(scopedEncoder.writer.lengthInBytes, size);
        writer.addAll(scopedEncoder.writer.chunks);

        break;
      case DataType.TUPLE:
        if (value is! Tuple) {
          throw new ArgumentError("Expected value for field '${name}' of type ${DataType.nameOf(valueType)} to be an instance of Tuple");
        }

        Iterable v = (value as Tuple);

        // Encode items into a separate buffer, then write the buffer length and buffer data
        SizeType itemSize = SizeType.LONG;
        TypeEncoder scopedEncoder = new TypeEncoder(protocolVersion);
        for (int index = 0; index < v.length; index++) {
          scopedEncoder.writeTypedValue(name,
          v.elementAt(index),
          typeSpec : typeSpec.tupleFields.elementAt(index), size: itemSize);
        }

        // Write buffer size in bytes and the actual buffer data
        writeLength(scopedEncoder.writer.lengthInBytes, size);
        writer.addAll(scopedEncoder.writer.chunks);
        break;
      default:
        throw new ArgumentError("Unsupported type ${DataType.nameOf(valueType)} for arg '${name}' with value ${value}");
    }
  }

//  void dumpToFile(String outputFile) {
//    File file = new File(outputFile);
//    file.writeAsStringSync('');
//    _writer._bufferedChunks.forEach((List<int> chunk) => file.writeAsBytesSync(chunk, mode : FileMode.APPEND));
//  }

  ChunkedOutputWriter get writer => _writer;
}
