## 0.1.5 (Feb 18, 2016)

Fixed another socket flush race condition ([#2](https://github.com/achilleasa/dart_cassandra_cql/pull/2))

## 0.1.4 (April 17, 2015)

Added the **preferBiggerTcpPackets** option (defaults to false). When enabled, the driver will
join together protocol frame chunks before piping them to the underlying TCP socket. This option
will improve performance at the expense of slightly higher memory consumption.

## 0.1.3 (December 20, 2014)

Improved support for compression codecs
Driver is now compatible with [dart_lz4](https://github.com/achilleasa/dart_lz4)

## 0.1.2 (December 6, 2014)

Renamed lib/driver to lib/src so that docgen works
Fixed race condition while flushing data to sockets ([#1](https://github.com/achilleasa/dart_cassandra_cql/issues/1))

## 0.1.1 (November 26, 2014)

Restructured folders to align with pub requirements

## 0.1.0 (November 24, 2014)

Initial release
