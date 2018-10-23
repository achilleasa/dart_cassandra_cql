part of dart_cassandra_cql.stream;

class TypeDecoder {
  int _offset = 0;
  ByteData _buffer;
  Endian endianess = Endian.big;
  ProtocolVersion protocolVersion;

  TypeDecoder.fromBuffer(
      ByteData this._buffer, ProtocolVersion this.protocolVersion);

  int readSignedByte() {
    return _buffer.getInt8(_offset++);
  }

  int readByte() {
    return _buffer.getUint8(_offset++);
  }

  int readUInt() {
    int val = _buffer.getUint32(_offset, endianess);
    _offset += 4;

    return val;
  }

  int readInt() {
    int val = _buffer.getInt32(_offset, endianess);
    _offset += 4;

    return val;
  }

  double readFloat() {
    double val = _buffer.getFloat32(_offset, endianess);
    _offset += 4;

    return val;
  }

  double readDouble() {
    double val = _buffer.getFloat64(_offset, endianess);
    _offset += 8;

    return val;
  }

  int readLong() {
    int val = _buffer.getInt64(_offset, endianess);
    _offset += 8;

    return val;
  }

  int readSignedShort() {
    int val = _buffer.getInt16(_offset, endianess);
    _offset += 2;

    return val;
  }

  int readShort() {
    int val = _buffer.getUint16(_offset, endianess);
    _offset += 2;

    return val;
  }

  int readLength(SizeType size) {
    return size == SizeType.LONG
        ? readInt()
        : (size == SizeType.SHORT ? readShort() : readByte());
  }

  void skipBytes(int len) {
    _offset += len;
  }

  void skipString(SizeType size) {
    int len = readLength(size);
    _offset += len;
  }

  String readAsciiString(SizeType size, [int len = null]) {
    if (len == null) {
      len = readLength(size);
      if (len < 0) {
        return null;
      }
    }
    _offset += len;
    return ascii.decode(new Uint8List.view(_buffer.buffer, _offset - len, len));
  }

  String readString(SizeType size, [int len = null]) {
    if (len == null) {
      len = readLength(size);
      if (len < 0) {
        return null;
      }
    }
    _offset += len;
    return utf8.decode(new Uint8List.view(_buffer.buffer, _offset - len, len));
  }

  Uint8List readBytes(SizeType size, [int len = null]) {
    if (len == null) {
      len = readLength(size);
      // Null is defined as a negative length
      if (len < 0) {
        return null;
      }
    }
    _offset += len;
    return new Uint8List.view(_buffer.buffer, _offset - len, len);
  }

  Consistency readConsistency() {
    return Consistency.valueOf(readShort());
  }

  List<String> readStringList(SizeType size) {
    int len = readLength(size);
    return new List.generate(len, (_) => readString(size));
  }

  Map<String, String> readStringMap(SizeType size) {
    int len = readLength(size);
    final map = <String, String>{};
    while (len-- > 0) {
      map[readString(size)] = readString(size);
    }
    return map;
  }

  Map<String, List<String>> readStringMultiMap(SizeType size) {
    int len = readLength(size);
    final map = <String, List<String>>{};
    while (len-- > 0) {
      map[readString(size)] = readStringList(size);
    }
    return map;
  }

  /**
   * Create a new [Header] object by parsing & verifying the protocol
   * contained in the supplied [buffer]. This method will throw a
   * [ParseException] if an error occurs while parsing header fields
   */

  FrameHeader readHeader() {
    FrameHeader header = new FrameHeader();

    // Parse and validate version
    int versionValue = readByte();
    try {
      header.version = HeaderVersion.valueOf(versionValue);
      switch (header.version) {
        case HeaderVersion.REQUEST_V2:
        case HeaderVersion.REQUEST_V3:
        case HeaderVersion.RESPONSE_V2:
        case HeaderVersion.RESPONSE_V3:
          break;
        default:
          throw new ArgumentError("Unsupported version value");
      }
    } on ArgumentError {
      throw new ArgumentError(
          "Unsupported server version value '0x${versionValue.toRadixString(16)}' while parsing frame header");
    }

    // Parse flags and stream id
    header.flags = readByte();
    header.streamId = header.version == HeaderVersion.REQUEST_V2 ||
            header.version == HeaderVersion.RESPONSE_V2
        ? readSignedByte()
        : readSignedShort();

    // Parse and validate opcode
    int opcodeValue = readByte();
    try {
      header.opcode = Opcode.valueOf(opcodeValue);
    } on ArgumentError {
      header.unknownOpcodeValue = opcodeValue;
    }

    // Parse length
    header.length = readUInt();

    return header;
  }

  /**
   * Read and decode VarInt. Returns the parsed [int] value
   * Based on: https://github.com/datastax/cpp-driver/blob/deprecated/src/cql/cql_varint.cpp
   * but exploits dart vm support for arbitary long ints to parse varInts of any size
   */

  BigInt readVarInt(SizeType size, [int len = null]) {
    if (len == null) {
      len = readLength(size);
      // Null is defined as a negative length
      if (len < 0) {
        return null;
      }
    }

    if (len < 1) {
      _offset += len;
      throw new ArgumentError(
          "Could not parse varint value with length ${len}");
    }

    // Read bytes
    Uint8List buf = readBytes(size, len);

    int bytesToCopy = buf.lengthInBytes;
    int bytesToFill = 1;
    Uint8List decodeBuffer = new Uint8List(bytesToCopy + bytesToFill);

    // Check the first actual digit (@ buf[0]) to figure out which
    // filler byte we will use for the decode buffer (controls the sign)
    decodeBuffer[0] = (buf[0] & 0x80) == 0x80 ? 0xFF : 0x00;

    // Copy bytes from original buffer to the decode buffer
    for (int i = 0; i < bytesToCopy; i++) {
      decodeBuffer[i + bytesToFill] = buf[i];
    }

    // Assemble final decoded number and apply sign
    final value = decodeBuffer.fold<BigInt>(BigInt.zero,
        (BigInt num, int byteValue) => (num << 8) + new BigInt.from(byteValue));
    return value.toSigned(8 * bytesToCopy);
  }

  /**
   * Read and decode variable precision decimal int/double. Returns either an [int] or a [double]
   * depending on the number of decimal points that the packed type contains.
   *
   * Based on: https://github.com/datastax/cpp-driver/blob/deprecated/src/cql/cql_decimal.cpp
   * but exploits dart vm support for arbitary long ints/doubles to parse decimals of any size
   */

  Object readDecimal(SizeType size, [int len = null]) {
    if (len == null) {
      len = readLength(size);
      // Null is defined as a negative length
      if (len < 0) {
        return null;
      }
    }

    // If we have less than 5 bytes then we cannot parse this
    if (len < 5) {
      _offset += len;
      throw new ArgumentError(
          "Could not parse decimal value with length ${len}");
    }

    // Read bytes
    Uint8List buf = readBytes(size, len);

    int bytesToCopy = buf.lengthInBytes - 4;
    int bytesToFill = 1;
    Uint8List decodeBuffer = new Uint8List(bytesToCopy + bytesToFill);

    // Check the first actual digit (@ buf[4]) to figure out which
    // filler byte we will use for the decode buffer (controls the sign)
    decodeBuffer[0] = (buf[4] & 0x80) == 0x80 ? 0xFF : 0x00;

    // Copy bytes from original buffer to the decode buffer
    for (int i = 0; i < bytesToCopy; i++) {
      decodeBuffer[i + bytesToFill] = buf[i + 4];
    }

    // Assemble final decoded number and apply sign
    int value = decodeBuffer.fold(
        0, (int num, int byteValue) => (num << 8) | byteValue);
    value = value.toSigned(8 * bytesToCopy);

    // Buf[3] specifies the number of fractional points. If it is 0 then this is just a wide int
    // Otherwise we need to convert it to a double and move the fractional point to the left
    return buf[3] == 0 ? value : value.toDouble() * pow(10, -buf[3]);
  }

  TypeSpec readTypeOption() {
    DataType type = DataType.valueOf(readShort());
    Object keyType = null;
    TypeSpec spec = null;

    // Collection types and custom type have additional
    // option parameters which we need to parse
    switch (type) {
      case DataType.CUSTOM:
        // Custom type java FQ class
        spec = new TypeSpec(type)..customTypeClass = readString(SizeType.SHORT);
        break;
      case DataType.LIST:
      case DataType.SET:
        // Value is an option representing the list item type
        spec = new TypeSpec(type,
            keySubType: keyType, valueSubType: readTypeOption());
        break;
      case DataType.MAP:
        // We have two option values, one for the map key type and one for the value type
        spec = new TypeSpec(type,
            keySubType: readTypeOption(), valueSubType: readTypeOption());
        break;
      case DataType.UDT:
        spec = new TypeSpec(type);
        spec.keyspace = readString(SizeType.SHORT);
        spec.udtName = readString(SizeType.SHORT);
        // numFields <String, TypeOption> tuples follow
        int numFields = readShort();
        for (int fieldIndex = 0; fieldIndex < numFields; fieldIndex++) {
          spec.udtFields[readString(SizeType.SHORT)] = readTypeOption();
        }
        break;
      case DataType.TUPLE:
        spec = new TypeSpec(type);
        // numFields <TypeOption> records follow
        int numFields = readShort();
        for (int fieldIndex = 0; fieldIndex < numFields; fieldIndex++) {
          spec.tupleFields.add(readTypeOption());
        }
        break;
      default:
        spec = new TypeSpec(type);
    }

    return spec;
  }

  Object readTypedValue(TypeSpec typeSpec, {SizeType size}) {
    // Read typed value length in bytes
    int lenInBytes = readLength(size);

    //_logger.fine("[TypeDecoder::readTypedValue] Attempting to read ${typeSpec} @ 0x${offset.toRadixString(16)} ($lenInBytes bytes long)");

    // Null values have negative length
    if (lenInBytes < 0) {
      return null;
    }

    switch (typeSpec.valueType) {
      case DataType.ASCII:
        return readAsciiString(size, lenInBytes);
      case DataType.TEXT:
      case DataType.VARCHAR:
        return readString(size, lenInBytes);
      case DataType.UUID:
      case DataType.TIMEUUID:
        return new Uuid.fromBytes(readBytes(size, lenInBytes));
      case DataType.CUSTOM:
        // If a codec has been specified for this type, use that; otherwise return the
        // serialized data as a Uint8 list
        Codec typeCodec = getCodec(typeSpec.customTypeClass);
        return typeCodec != null
            ? typeCodec.decode(readBytes(size, lenInBytes))
            : readBytes(size, lenInBytes);
      case DataType.BLOB:
        return readBytes(size, lenInBytes);
      case DataType.INT:
        return readInt();
      case DataType.BIGINT:
      case DataType.COUNTER:
        return readLong();
      case DataType.TIMESTAMP:
        return new DateTime.fromMillisecondsSinceEpoch(readLong());
      case DataType.BOOLEAN:
        return readByte() != 0;
      case DataType.FLOAT:
        return readFloat();
      case DataType.DOUBLE:
        return readDouble();
      case DataType.INET:
        // INET can be either 4 (ipv4) or 16 (ipv6) bytes long
        if (lenInBytes == 4) {
          Uint8List buf = readBytes(SizeType.BYTE, lenInBytes);
          return new InternetAddress(buf.join("."));
        } else if (lenInBytes == 16) {
          return new InternetAddress(
              new List<String>.generate(8, (_) => readShort().toRadixString(16))
                  .join(":"));
        } else {
          throw new Exception(
              "Could not decode INET type of length ${lenInBytes}");
        }
        break;
      case DataType.LIST:
        SizeType itemSize = protocolVersion == ProtocolVersion.V2
            ? SizeType.SHORT
            : SizeType.LONG;

        int len = itemSize == SizeType.SHORT ? readShort() : readInt();

        // The spec defines a list as a short (num of elements) followed by N typeSpec.value records.
        // Each record is a <short(V2)>/<int(V3)> length followed by M bytes.
        return new List.generate(
            len, (_) => readTypedValue(typeSpec.valueSubType, size: itemSize));
      case DataType.SET:
        SizeType itemSize = protocolVersion == ProtocolVersion.V2
            ? SizeType.SHORT
            : SizeType.LONG;

        int entry = itemSize == SizeType.SHORT ? readShort() : readInt();

        // The spec defines a set as a short (num of elements) followed by N typeSpec.value records
        // Each record is a <short(V2)>/<int(V3)> length followed by M bytes.
        Set set = new Set();
        for (; entry > 0; entry--) {
          set.add(readTypedValue(typeSpec.valueSubType, size: itemSize));
        }
        return set;
      case DataType.MAP:
        SizeType itemSize = protocolVersion == ProtocolVersion.V2
            ? SizeType.SHORT
            : SizeType.LONG;

        // The spec defines a map as a short (num of elements) followed by N <typeSpec.value, typeSpec.value2> record pairs.
        Map map = new LinkedHashMap();
        int pair = itemSize == SizeType.SHORT ? readShort() : readInt();

        for (; pair > 0; pair--) {
          map[readTypedValue(typeSpec.keySubType, size: itemSize)] =
              readTypedValue(typeSpec.valueSubType, size: itemSize);
        }
        return map;
      case DataType.DECIMAL:
        return readDecimal(size, lenInBytes);
      case DataType.VARINT:
        return readVarInt(size, lenInBytes);
      case DataType.UDT:
        Map udt = new LinkedHashMap();
        typeSpec.udtFields.forEach((String name, TypeSpec udtSpec) =>
            udt[name] = readTypedValue(udtSpec, size: size));
        return udt;
      case DataType.TUPLE:
        Tuple tuple = new Tuple.fromIterable(new List.generate(
            typeSpec.tupleFields.length,
            (int fieldIndex) =>
                readTypedValue(typeSpec.tupleFields[fieldIndex], size: size)));
        return tuple;
      default:
        skipBytes(lenInBytes);
        return null;
    }
  }

  void dumpToFile(String outputFile) {
    new File(outputFile)..writeAsBytesSync(_buffer.buffer.asInt8List());
  }

  get offset => _offset;
}
