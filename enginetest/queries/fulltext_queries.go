// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queries

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var FulltextTests = []ScriptTest{
	{
		Name: "Basic matching 1 PK",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT pk, v1 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi"}},
			},
			{
				Query:    "SELECT v1, v2 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{"ghi", "jkl"}},
			},
			{
				Query:    "SELECT pk, v1, v2 FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT pk, v2 FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				Expected: []sql.Row{{uint64(2), "jkl"}},
			},
			{
				Query:    "SELECT v1 FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				Expected: []sql.Row{{"ghi"}},
			},
			{
				Query:    "SELECT v2 FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				Expected: []sql.Row{{"jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl') = 0;",
				Expected: []sql.Row{{uint64(1), "abc", "def pqr"}, {uint64(3), "mno", "mno"}, {uint64(4), "stu vwx", "xyz zyx yzx"}, {uint64(5), "ghs", "mno shg"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl') > 0;",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno') AND pk = 3;",
				Expected: []sql.Row{{uint64(3), "mno", "mno"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno') OR pk = 1;",
				Expected: []sql.Row{{uint64(1), "abc", "def pqr"}, {uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 1 UK",
		SetUpScript: []string{
			"CREATE TABLE test (uk BIGINT UNSIGNED NOT NULL UNIQUE, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT uk, v1 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi"}},
			},
			{
				Query:    "SELECT uk, v2, v1 FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				Expected: []sql.Row{{uint64(2), "jkl", "ghi"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching No Keys",
		SetUpScript: []string{
			"CREATE TABLE test (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES ('abc', 'def pqr'), ('ghi', 'jkl'), ('mno', 'mno'), ('stu vwx', 'xyz zyx yzx'), ('ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{"ghi", "jkl"}},
			},
			{
				Query:    "SELECT v1 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{"ghi"}},
			},
			{
				Query:    "SELECT v2 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{"jkl"}},
			},
			{
				Query:    "SELECT v2, v1 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{"jkl", "ghi"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				Expected: []sql.Row{{"ghi", "jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{"ghi", "jkl"}, {"mno", "mno"}, {"ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 PKs",
		SetUpScript: []string{
			"CREATE TABLE test (pk1 BIGINT UNSIGNED, pk2 BIGINT UNSIGNED, v1 VARCHAR(200), v2 VARCHAR(200), PRIMARY KEY (pk1, pk2), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 1, 'abc', 'def pqr'), (2, 1, 'ghi', 'jkl'), (3, 1, 'mno', 'mno'), (4, 1, 'stu vwx', 'xyz zyx yzx'), (5, 1, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), uint64(1), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), uint64(1), "ghi", "jkl"}, {uint64(3), uint64(1), "mno", "mno"}, {uint64(5), uint64(1), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 PKs Reversed",
		SetUpScript: []string{
			"CREATE TABLE test (pk1 BIGINT UNSIGNED, pk2 BIGINT UNSIGNED, v1 VARCHAR(200), v2 VARCHAR(200), PRIMARY KEY (pk2, pk1), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 1, 'abc', 'def pqr'), (2, 1, 'ghi', 'jkl'), (3, 1, 'mno', 'mno'), (4, 1, 'stu vwx', 'xyz zyx yzx'), (5, 1, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), uint64(1), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), uint64(1), "ghi", "jkl"}, {uint64(3), uint64(1), "mno", "mno"}, {uint64(5), uint64(1), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 PKs Non-Sequential",
		SetUpScript: []string{
			"CREATE TABLE test (pk1 BIGINT UNSIGNED, v1 VARCHAR(200), pk2 BIGINT UNSIGNED, v2 VARCHAR(200), PRIMARY KEY (pk2, pk1), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 1, 'def pqr'), (2, 'ghi', 1, 'jkl'), (3, 'mno', 1, 'mno'), (4, 'stu vwx', 1, 'xyz zyx yzx'), (5, 'ghs', 1, 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", uint64(1), "jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), "ghi", uint64(1), "jkl"}, {uint64(3), "mno", uint64(1), "mno"}, {uint64(5), "ghs", uint64(1), "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 UKs",
		SetUpScript: []string{
			"CREATE TABLE test (uk1 BIGINT UNSIGNED NOT NULL, uk2 BIGINT UNSIGNED NOT NULL, v1 VARCHAR(200), v2 VARCHAR(200), UNIQUE KEY (uk1, uk2), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 1, 'abc', 'def pqr'), (2, 1, 'ghi', 'jkl'), (3, 1, 'mno', 'mno'), (4, 1, 'stu vwx', 'xyz zyx yzx'), (5, 1, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), uint64(1), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT v2, uk2 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{"jkl", uint64(1)}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), uint64(1), "ghi", "jkl"}, {uint64(3), uint64(1), "mno", "mno"}, {uint64(5), uint64(1), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 UKs Reversed",
		SetUpScript: []string{
			"CREATE TABLE test (uk1 BIGINT UNSIGNED NOT NULL, uk2 BIGINT UNSIGNED NOT NULL, v1 VARCHAR(200), v2 VARCHAR(200), UNIQUE KEY (uk2, uk1), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 1, 'abc', 'def pqr'), (2, 1, 'ghi', 'jkl'), (3, 1, 'mno', 'mno'), (4, 1, 'stu vwx', 'xyz zyx yzx'), (5, 1, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), uint64(1), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), uint64(1), "ghi", "jkl"}, {uint64(3), uint64(1), "mno", "mno"}, {uint64(5), uint64(1), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 UKs Non-Sequential",
		SetUpScript: []string{
			"CREATE TABLE test (uk1 BIGINT UNSIGNED NOT NULL, v1 VARCHAR(200), uk2 BIGINT UNSIGNED NOT NULL, v2 VARCHAR(200), UNIQUE KEY (uk1, uk2), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 1, 'def pqr'), (2, 'ghi', 1, 'jkl'), (3, 'mno', 1, 'mno'), (4, 'stu vwx', 1, 'xyz zyx yzx'), (5, 'ghs', 1, 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", uint64(1), "jkl"}},
			},
			{
				Query:    "SELECT v2, uk2 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{"jkl", uint64(1)}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), "ghi", uint64(1), "jkl"}, {uint64(3), "mno", uint64(1), "mno"}, {uint64(5), "ghs", uint64(1), "mno shg"}},
			},
		},
	},
	{
		Name: "CREATE INDEX before insertions",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200));",
			"CREATE FULLTEXT INDEX idx ON test (v1, v2);",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "CREATE INDEX after insertions",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
			"CREATE FULLTEXT INDEX idx ON test (v1, v2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "ALTER TABLE CREATE INDEX before insertions",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200));",
			"ALTER TABLE test ADD FULLTEXT INDEX idx (v1, v2);",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "ALTER TABLE CREATE INDEX after insertions",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
			"ALTER TABLE test ADD FULLTEXT INDEX idx (v1, v2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "DROP INDEX",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "DROP INDEX idx ON test;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				ExpectedErr: sql.ErrNoFullTextIndexFound,
			},
		},
	},
	{
		Name: "ALTER TABLE DROP INDEX",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
			"CREATE FULLTEXT INDEX idx ON test (v1, v2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "ALTER TABLE test DROP INDEX idx;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				ExpectedErr: sql.ErrNoFullTextIndexFound,
			},
		},
	},
	{
		Name: "ALTER TABLE ADD COLUMN",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test ADD COLUMN v3 FLOAT DEFAULT 7 FIRST;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{float32(7), uint64(2), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN not used by index",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), v3 BIGINT UNSIGNED, FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr', 7), (2, 'ghi', 'jkl', 7), (3, 'mno', 'mno', 7), (4, 'stu vwx', 'xyz zyx yzx', 7), (5, 'ghs', 'mno shg', 7);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test MODIFY COLUMN v3 FLOAT AFTER pk;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), float32(7), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN used by index to valid type",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test MODIFY COLUMN v2 TEXT;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN used by index to invalid type",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE test MODIFY COLUMN v2 VARBINARY(200);",
				ExpectedErr: sql.ErrFullTextInvalidColumnType,
			},
		},
	},
	{
		Name: "ALTER TABLE DROP COLUMN not used by index",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), v3 BIGINT UNSIGNED, FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr', 7), (2, 'ghi', 'jkl', 7), (3, 'mno', 'mno', 7), (4, 'stu vwx', 'xyz zyx yzx', 7), (5, 'ghs', 'mno shg', 7);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test DROP COLUMN v3;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE DROP COLUMN used by index",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE test DROP COLUMN v2;",
				ExpectedErr: sql.ErrFullTextMissingColumn,
			},
		},
	},
	{
		Name: "ALTER TABLE ADD PRIMARY KEY",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test ADD PRIMARY KEY (pk);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE DROP PRIMARY KEY",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test DROP PRIMARY KEY;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{uint64(2), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE DROP PRIMARY KEY",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{ // This is mainly to check for a panic
				Query:    "DROP TABLE test;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "No prefix needed for TEXT columns",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE TABLE `film_text` (`film_id` SMALLINT NOT NULL, `title` VARCHAR(255) NOT NULL, `description` TEXT, PRIMARY KEY (`film_id`), FULLTEXT KEY `idx_title_description` (`title`,`description`));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Rename new table to match old table",
		SetUpScript: []string{
			"CREATE TABLE test1 (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test1 VALUES ('abc', 'def');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "RENAME TABLE test1 TO test2;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM test2 WHERE MATCH(v1, v2) AGAINST ('abc');",
				Expected: []sql.Row{{"abc", "def"}},
			},
			{
				Query:    "CREATE TABLE test1 (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO test1 VALUES ('ghi', 'jkl');",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM test1 WHERE MATCH(v1, v2) AGAINST ('abc');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM test2 WHERE MATCH(v1, v2) AGAINST ('abc');",
				Expected: []sql.Row{{"abc", "def"}},
			},
			{
				Query:    "SELECT * FROM test1 WHERE MATCH(v1, v2) AGAINST ('jkl');",
				Expected: []sql.Row{{"ghi", "jkl"}},
			},
			{
				Query:    "SELECT * FROM test2 WHERE MATCH(v1, v2) AGAINST ('jkl');",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "Rename index",
		SetUpScript: []string{
			"CREATE TABLE test (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v2, v1));",
			"INSERT INTO test VALUES ('abc', 'def');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `v1` varchar(200),\n  `v2` varchar(200),\n  FULLTEXT KEY `idx` (`v2`,`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER TABLE test RENAME INDEX idx TO new_idx;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('abc');",
				Expected: []sql.Row{{"abc", "def"}},
			},
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `v1` varchar(200),\n  `v2` varchar(200),\n  FULLTEXT KEY `new_idx` (`v2`,`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "Multiple overlapping indexes",
		SetUpScript: []string{
			"CREATE TABLE test (v1 TEXT, v2 VARCHAR(200), v3 MEDIUMTEXT, FULLTEXT idx1 (v1, v2), FULLTEXT idx2 (v1, v3), FULLTEXT idx3 (v2, v3));",
			"INSERT INTO test VALUES ('abc', 'def', 'ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('abc');",
				Expected: []sql.Row{{"abc", "def", "ghi"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('def');",
				Expected: []sql.Row{{"abc", "def", "ghi"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v3) AGAINST ('abc');",
				Expected: []sql.Row{{"abc", "def", "ghi"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v3) AGAINST ('def');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v3) AGAINST ('ghi');",
				Expected: []sql.Row{{"abc", "def", "ghi"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v3) AGAINST ('abc');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v3) AGAINST ('def');",
				Expected: []sql.Row{{"abc", "def", "ghi"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v3) AGAINST ('ghi');",
				Expected: []sql.Row{{"abc", "def", "ghi"}},
			},
		},
	},
	{
		Name: "Duplicate column names",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE test (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v1));",
				ExpectedErr: sql.ErrFullTextDuplicateColumn,
			},
		},
	},
	{
		Name: "References missing column",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE test (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v3));",
				ExpectedErr: sql.ErrUnknownIndexColumn,
			},
		},
	},
	{
		Name: "Creating an index on an invalid type",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE test (v1 VARCHAR(200), v2 BIGINT, FULLTEXT idx (v1, v2));",
				ExpectedErr: sql.ErrFullTextInvalidColumnType,
			},
		},
	},
}