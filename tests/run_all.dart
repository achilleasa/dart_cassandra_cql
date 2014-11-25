library dart_cassandra_cql.tests;

import "lib/enum_test.dart" as enums;
import "lib/query_test.dart" as query;
import "lib/chunked_input_reader_test.dart" as chunkedInputReader;
import "lib/serialization_test.dart" as serialization;
import 'lib/frame_parser_test.dart' as frameParser;
import "lib/frame_writer_test.dart" as frameWriter;
import "lib/connection_test.dart" as connection;
import "lib/type_test.dart" as typeTest;
import "lib/pool_config_test.dart" as poolConfig;
import "lib/client_test.dart" as client;

void main(List<String> args) {
  String allArgs = args.join(".");
  bool runAll = args.isEmpty;

  //useCompactVMConfiguration();

  if (runAll || (new RegExp("enums")).hasMatch(allArgs)) {
    enums.main();
  }

  if (runAll || (new RegExp("chunked-input-reader")).hasMatch(allArgs)) {
    chunkedInputReader.main();
  }

  if (runAll || (new RegExp("serialization")).hasMatch(allArgs)) {
    serialization.main();
  }

  if (runAll || (new RegExp("frame-parser")).hasMatch(allArgs)) {
    frameParser.main();
  }

  if (runAll || (new RegExp("frame-writer")).hasMatch(allArgs)) {
    frameWriter.main();
  }

  if (runAll || (new RegExp("connection")).hasMatch(allArgs)) {
    connection.main();
  }

  if (runAll || (new RegExp("type-test")).hasMatch(allArgs)) {
    typeTest.main();
  }

  if (runAll || (new RegExp("pool-config")).hasMatch(allArgs)) {
    poolConfig.main();
  }

  if (runAll || (new RegExp("query")).hasMatch(allArgs)) {
    query.main();
  }

  if (runAll || (new RegExp("client")).hasMatch(allArgs)) {
    client.main();
  }

}