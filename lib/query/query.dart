part of dart_cassandra_cql.query;

class Query extends QueryInterface {

  bool prepared;
  String _query;
  String _positionalQuery;
  Object _bindings;
  List<String> _namedToPositionalBindings;
  Consistency consistency;
  Consistency serialConsistency;
  static final List<String> _byteToChar = [
      "0", "1", "2", "3",
      "4", "5", "6", "7",
      "8", "9", "a", "b",
      "c", "d", "e", "f"
  ];

  Query(String this._query, {
  Object bindings : null
  , Consistency this.consistency : Consistency.QUORUM
  , Consistency this.serialConsistency
  , bool this.prepared : false
  }) {
    this.bindings = bindings;
  }

  /**
   * Returns a [String] with the original query where all positional or named placeholders
   * are expanded to the values of supplied bindings
   */

  String get expandedQuery {
    StringBuffer buffer = new StringBuffer();
    // If no bindings are specified return the original query string
    if (_bindings == null) {
      return _query;
    } else if (_bindings is Iterable) {
      _expandPositionalPlaceholders(buffer);
    } else {
      _expandNamedPlaceholders(buffer);
    }
    return buffer.toString();
  }

  String get query => _query;

  String get positionalQuery {
    // Already converted
    if (_positionalQuery != null) {
      return _positionalQuery;
    }

    _namedToPositionalBindings = new List<String>();
    StringBuffer buffer = new StringBuffer();
    int blockStart = 0;
    int offset = 0;
    bool insideLiteral = false;
    RegExp placeholderRegex = new RegExp(":[a-zA-Z0-9_]+", caseSensitive: false);
    for (; offset < _query.length; offset++) {
      if (_query[offset] == "'") {
        insideLiteral = !insideLiteral;
        continue;
      }

      // Found named placeholder while not inside a literal block
      if (_query[offset] == ":" && !insideLiteral) {
        // If we were capturing a block add it to the chunk list
        if (offset - 1 - blockStart > 0) {
          buffer.write(_query.substring(blockStart, offset));
        }

        // Capture placeholder name
        Match placeholderMatch = placeholderRegex.matchAsPrefix(_query, offset);
        if (placeholderMatch == null) {
          throw new ArgumentError("Expected named placeholder to begin at offset $offset");
        }
        String name = placeholderMatch.group(0).substring(1);

        // Replace named binding with positional placeholder
        buffer.write('?');
        _namedToPositionalBindings.add(name);

        // Begin capturing a new block after the placeholder name
        offset += name.length;
        blockStart = offset + 1;
      }
    }

    // Write final captured block (if any)
    if (offset - blockStart > 0) {
      buffer.write(_query.substring(blockStart));
    }

    _positionalQuery = buffer.toString();
    return _positionalQuery;
  }

  Object get bindings => _bindings;

  set bindings(Object value) {
    if (value != null && (value is! Iterable) && (value is! Map)) {
      throw new ArgumentError("Bindings should be either an Iterable or a Map");
    }
    this._bindings = value;
  }

  List<Object> get namedToPositionalBindings {
    // Query specifies positional bindings
    if (_bindings is List) {
      return _bindings;
    }

    if (_namedToPositionalBindings == null || _bindings == null) {
      return null;
    }

    // Map named bindings to positional
    Map bindingsMap = _bindings as Map;
    return new List.generate(_namedToPositionalBindings.length, (argIndex) {
      String name = _namedToPositionalBindings[argIndex];
      if (!bindingsMap.containsKey(name)) {
        throw new ArgumentError("Missing binding for named placeholder '$name'");
      }
      return bindingsMap[ name ];
    });
  }

  void _expandPositionalPlaceholders(StringBuffer buffer) {
    List<Object> bindingList = _bindings as Iterable;
    int bindingIndex = 0;
    int blockStart = 0;
    int offset = 0;
    bool insideLiteral = false;
    for (; offset < _query.length; offset++) {
      if (_query[offset] == "'") {
        insideLiteral = !insideLiteral;
        continue;
      }

      // Found positional placeholder while not inside a literal block
      if (_query[offset] == "?" && !insideLiteral) {
        // If we were capturing a block add it to the chunk list
        if (offset - 1 - blockStart > 0) {
          buffer.write(_query.substring(blockStart, offset));
        }

        if (bindingList.length <= bindingIndex) {
          throw new ArgumentError("Missing argument '${bindingIndex}' from bindings list");
        }

        buffer.write(
            _typeToString(bindingList[bindingIndex++])
        );

        // Begin capturing a new block after the placeholder char
        blockStart = offset + 1;
      }
    }

    // Write final captured block (if any)
    if (offset - blockStart > 0) {
      buffer.write(_query.substring(blockStart));
    }
  }

  void _expandNamedPlaceholders(StringBuffer buffer) {
    Map<String, Object> bindingMap = _bindings as Map;
    int blockStart = 0;
    int offset = 0;
    bool insideLiteral = false;
    RegExp placeholderRegex = new RegExp(":[a-zA-Z0-9_]+", caseSensitive: false);
    for (; offset < _query.length; offset++) {
      if (_query[offset] == "'") {
        insideLiteral = !insideLiteral;
        continue;
      }

      // Found named placeholder while not inside a literal block
      if (_query[offset] == ":" && !insideLiteral) {
        // If we were capturing a block add it to the chunk list
        if (offset - 1 - blockStart > 0) {
          buffer.write(_query.substring(blockStart, offset));
        }

        // Capture placeholder name
        Match placeholderMatch = placeholderRegex.matchAsPrefix(_query, offset);
        if (placeholderMatch == null) {
          throw new ArgumentError("Expected named placeholder to begin at offset $offset");
        }
        String name = placeholderMatch.group(0).substring(1);
        if (!bindingMap.containsKey(name)) {
          throw new ArgumentError("Missing binding for named placeholder '$name'");
        }

        // Stringify binding value
        buffer.write(_typeToString(bindingMap[name]));

        // Begin capturing a new block after the placeholder name
        offset += name.length;
        blockStart = offset + 1;
      }
    }

    // Write final captured block (if any)
    if (offset - blockStart > 0) {
      buffer.write(_query.substring(blockStart));
    }
  }

  StringBuffer _bytesToHex(Uint8List bytes) {
    StringBuffer buffer = new StringBuffer();
    buffer.write("0x");
    bytes.forEach((int b) {
      buffer.write(_byteToChar[(b & 0xf0) >> 4]);
      buffer.write(_byteToChar[b & 0x0f]);
    });
    return buffer;
  }

  Object _typeToString(Object value, {quoteStrings : true}) {
    if (value == null) {
      return "null";
    } else if (value is String) {
      StringBuffer buffer = new StringBuffer();
      buffer.write(r"'");
      // Escape single quotes
      buffer.write(value.replaceAll(r"'", r"''"));
      buffer.write(r"'");
      return buffer;
    } else if (value is DateTime) {
      return value.millisecondsSinceEpoch.toString();
    } else if (value is InternetAddress) {
      return "'${value.address}'";
    } else if (value is Uuid) {
      return value.value;
    } else if (value is CustomType) {
      if (value == null) {
        return "null";
      }

      Codec<Object, Uint8List> codec = getCodec(value.customTypeClass);
      if (codec == null) {
        throw new ArgumentError("No custom type codec specified for type with class: ${value.customTypeClass}");
      }

      StringBuffer buffer = new StringBuffer();
      codec.encode(value).forEach(buffer.writeCharCode);

      return new StringBuffer()
        ..write(r"'")
        ..write(buffer.toString().replaceAll(r"'", r"''"))
        ..write(r"'");

    } else if (value is TypedData) {
      StringBuffer buffer = new StringBuffer();
      Uint8List v = new Uint8List.view(value.buffer, 0, value.lengthInBytes);

      if (v.lengthInBytes == 0) {
        return "null";
      }

      return _bytesToHex(v);
    } else if (value is Tuple) {
      Tuple tuple = value;
      StringBuffer buffer = new StringBuffer();
      buffer.write("(");
      if (tuple != null) {
        buffer.write(tuple.map(_typeToString).join(","));
      }
      buffer.write(")");
      return buffer;
    } else if (value is Iterable) {
      StringBuffer buffer = new StringBuffer();
      buffer.write("[");
      buffer.writeAll(value.map(_typeToString), ",");
      buffer.write("]");
      return buffer;
    } else if (value is Map) {
      Map map = new LinkedHashMap();
      value.forEach((Object k, Object v) {
        map[ _typeToString(k) ] = _typeToString(v);
      });
      return map;
    } else {
      return value.toString();
    }
  }

}
