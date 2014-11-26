DROP KEYSPACE IF EXISTS test;

CREATE KEYSPACE IF NOT EXISTS test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE test;

//CREATE TYPE phone (
//      number text,
//      tags set<text>
//);
//
//CREATE TYPE address (
//      street text,
//      city text,
//      zip int,
//      phones set<phone>
//  );
//
//CREATE TABLE test.user_profiles (
//      login text PRIMARY KEY,
//      first_name text,
//      last_name text,
//      email text,
//      addresses map<text, address>
//  );
//      
////  // Inserts a user with a home address
//INSERT INTO user_profiles(login, first_name, last_name, email, addresses)
//VALUES (
//	  'test_user',
//      'Test',
//      'User',
//      'tuser@test.com',
//      { 'home': 
//      	{ street: '123 Test Str.',
//          city: 'San Fransisco',
//          zip: 94110,
//          phones : {
//           {number: '123 444 5555', tags: { 'preferred', 'direct line' }},
//           {number: '123 444 6666', tags: { 'fax' }}
//         }
//       }
//      });


CREATE TABLE test.type_test 
(
	ascii_type ascii,
	bigint_type bigint,
	blob_type blob,
	bool_type boolean,
	decimal_type decimal,
	double_type double,
	float_type float,
	inet_type inet,
	int_type int,
	list_type list<int>,
	map_type map<timestamp, text>,
	set_type set<int>,
	text_type text,
	timestamp_type timestamp,
	uuid_type uuid,
	timeuuid_type timeuuid,
	varchar_type varchar,
	varint_type varInt,
	
	PRIMARY KEY (uuid_type)
);

insert into test.type_test (
	ascii_type, bigint_type,  decimal_type, bool_type,
	double_type, float_type, inet_type, int_type, list_type, map_type,
	set_type, text_type, timestamp_type, uuid_type, timeuuid_type,
	varchar_type, varint_type, blob_type
) values(
	'text1', 9223372036854775807, 382123123.14, true,
	0.2342348, 3.14, '192.168.169.101', 100, [100, 200], {'2012-09-24' : 'the test', '2012-09-25' : 'the result'},
	{100, 200}, 'This is a long UTF8 ????????', dateof(now()), 550e8400-e29b-41d4-a716-446655440000, now(),
	'Arbitary long text goes here', 3123091212904812093120938120938120312890, 0xBADC0FFEEBADF00DD3ADBEEFABCD1234567890
);

insert into test.type_test (
	ascii_type, bigint_type, decimal_type, bool_type,
	double_type, float_type, inet_type, int_type, list_type, map_type,
	set_type, text_type, timestamp_type, uuid_type, timeuuid_type,
	varchar_type, varint_type, blob_type
) values(
	'text2', 123, 3123091212904812093120938120938120312890.3821, true,
	0.2342348, 3.14, '192.168.169.102', 32238493, [100, 200], {'2012-09-24' : 'the test', '2012-09-25' : 'the result'},
	{100, 200}, 'This is a long UTF8 ????????', dateof(now()), 550e8400-e29b-41d4-a716-446655440000, now(),
	'Arbitary long text goes here', 30312890, 0xfeedface
);

insert into test.type_test (
	ascii_type, bigint_type, decimal_type, bool_type,
	double_type, float_type, inet_type, int_type, list_type, map_type,
	set_type, text_type, timestamp_type, uuid_type, timeuuid_type,
	varchar_type, varint_type, blob_type
) values(
	'text3', 123, -1024.567438, true,
	0.2342348, 3.14, '192.168.169.102', 32238493, [100, 200], {'2012-09-24' : 'the test', '2012-09-25' : 'the result'},
	{100, 200}, 'This is a long UTF8 ????????', dateof(now()), 550e8400-e29b-41d4-a716-446655440000, now(),
	'Arbitary long text goes here', 30312890, 0xfeedface
);

insert into test.type_test (
	ascii_type, bigint_type, decimal_type, bool_type,
	double_type, float_type, inet_type, int_type, list_type, map_type,
	set_type, text_type, timestamp_type, uuid_type, timeuuid_type,
	varchar_type, varint_type, blob_type
) values(
	'text4', 123, -3821, true,
	0.2342348, 3.14, '192.168.169.102', 32238493, [100, 200], {'2012-09-24' : 'the test', '2012-09-25' : 'the result'},
	{100, 200}, 'This is a long UTF8 ????????', dateof(now()), 550e8400-e29b-41d4-a716-446655440000, now(),
	'Arbitary long text goes here', -30312890, 0xfeedface
);

CREATE TABLE test.page_view_counts
  (counter_value counter,
  url_name varchar,
  page_name varchar,
  PRIMARY KEY (url_name, page_name)
);
UPDATE test.page_view_counts SET counter_value = counter_value + 1 WHERE url_name='www.datastax.com' AND page_name='home';
UPDATE test.page_view_counts SET counter_value = counter_value + 1 WHERE url_name='www.foo.com' AND page_name='list';
UPDATE test.page_view_counts SET counter_value = counter_value + 1 WHERE url_name='www.foo.com' AND page_name='alerts';
UPDATE test.page_view_counts SET counter_value = counter_value + 1 WHERE url_name='www.datastax.com' AND page_name='products';
UPDATE test.page_view_counts SET counter_value = counter_value + 1 WHERE url_name='www.datastax.com' AND page_name='docs';
UPDATE test.page_view_counts SET counter_value = counter_value + 1 WHERE url_name='www.datastax.com' AND page_name='help';
UPDATE test.page_view_counts SET counter_value = counter_value + 1 WHERE url_name='www.boo.com' AND page_name='home';
UPDATE test.page_view_counts SET counter_value = counter_value + 1 WHERE url_name='www.boo.com' AND page_name='places';
UPDATE test.page_view_counts SET counter_value = counter_value + 1 WHERE url_name='www.boo.com' AND page_name='faq';
UPDATE test.page_view_counts SET counter_value = counter_value + 1 WHERE url_name='www.boo.com' AND page_name='contact';
 
select * from test.page_view_counts;
