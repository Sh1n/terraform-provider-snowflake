package snowflake

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUdf(t *testing.T) {
	r := require.New(t)
	db := "some_database"
	schema := "some_schema"
	udf := "test"

	v := Udf(udf).WithDB(db).WithSchema(schema)
	r.NotNil(v)
	r.False(v.secure)

	qn, err := v.QualifiedName()
	r.NoError(err)
	r.Equal(qn, fmt.Sprintf(`"%v"."%v"."%v"`, db, schema, udf))

	v.WithSecure()
	r.True(v.secure)

	// v.WithComment("great' comment")
	v.WithBody("return {...spread, index};")
	r.Equal("return {...spread, index};", v.body)

	args := []Argument{
		{
			name:  "arg1",
			_type: "OBJECT",
		},
		{
			name:  "arg12",
			_type: "VARCHAR",
		},
	}

	v.WithArguments(Arguments(args))

	v.WithReturnType("VARIANT")
	r.Equal("VARIANT", v.returnType)

	v.WithBody("PARSE_JSON(BASE64_DECODE_STRING(parent:child::string))")

	q, err := v.Create()
	r.NoError(err)
	r.Equal(`CREATE SECURE FUNCTION "some_database"."some_schema"."test" ("arg1" OBJECT, "arg12" VARCHAR) RETURNS VARIANT AS $$ PARSE_JSON(BASE64_DECODE_STRING(parent:child::string)) $$`, q)

	q, err = v.Secure()
	r.NoError(err)
	r.Equal(`ALTER FUNCTION "some_database"."some_schema"."test" (OBJECT, VARCHAR) SET SECURE`, q)

	q, err = v.Unsecure()
	r.NoError(err)
	r.Equal(`ALTER FUNCTION "some_database"."some_schema"."test" (OBJECT, VARCHAR) UNSET SECURE`, q)

	// q, err = v.ChangeComment("bad' comment")
	// r.NoError(err)
	// r.Equal(`ALTER VIEW "some_database"."some_schema"."test" SET COMMENT = 'bad\' comment'`, q)

	// q, err = v.RemoveComment()
	// r.NoError(err)
	// r.Equal(`ALTER VIEW "some_database"."some_schema"."test" UNSET COMMENT`, q)

	q, err = v.Drop()
	r.NoError(err)
	r.Equal(`DROP FUNCTION "some_database"."some_schema"."test" (OBJECT, VARCHAR)`, q)

	// q = v.Show()
	// r.Equal(`SHOW VIEWS LIKE 'test' IN SCHEMA "some_database"."some_schema"`, q)

	// v.WithDB("mydb")
	// qn, err = v.QualifiedName()
	// r.NoError(err)
	// r.Equal(qn, `"mydb"."some_schema"."test"`)

	// q, err = v.Create()
	// r.NoError(err)
	// r.Equal(`CREATE SECURE VIEW "mydb"."some_schema"."test" COMMENT = 'great\' comment' AS SELECT * FROM DUMMY WHERE blah = 'blahblah' LIMIT 1`, q)

	// q, err = v.Secure()
	// r.NoError(err)
	// r.Equal(`ALTER VIEW "mydb"."some_schema"."test" SET SECURE`, q)

	// q = v.Show()
	// r.Equal(`SHOW VIEWS LIKE 'test' IN SCHEMA "mydb"."some_schema"`, q)

	// q, err = v.Drop()
	// r.NoError(err)
	// r.Equal(`DROP VIEW "mydb"."some_schema"."test"`, q)
}

func TestUdfQualifiedName(t *testing.T) {
	r := require.New(t)
	v := Udf("udf").WithDB("db").WithSchema("schema")
	qn, err := v.QualifiedName()
	r.NoError(err)
	r.Equal(qn, `"db"."schema"."udf"`)
}

func TestUdfRename(t *testing.T) {
	r := require.New(t)
	v := Udf("test").WithDB("db").WithSchema("schema")

	q, err := v.Rename("test2")
	r.NoError(err)
	r.Equal(`ALTER FUNCTION "db"."schema"."test" () RENAME TO "db"."schema"."test2"`, q)

	v.WithDB("testDB")
	q, err = v.Rename("test3")
	r.NoError(err)
	r.Equal(`ALTER FUNCTION "testDB"."schema"."test2" () RENAME TO "testDB"."schema"."test3"`, q)

	v = Udf("test4").WithDB("db").WithSchema("testSchema")
	q, err = v.Rename("test5")
	r.NoError(err)
	r.Equal(`ALTER FUNCTION "db"."testSchema"."test4" () RENAME TO "db"."testSchema"."test5"`, q)
}
