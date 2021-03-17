package snowflake

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// Arguments Definition
type Argument struct {
	name  string
	_type string // type is reserved
}

func (c *Argument) WithName(name string) *Argument {
	c.name = name
	return c
}
func (c *Argument) WithType(t string) *Argument {
	c._type = t
	return c
}

func (c *Argument) getArgumentDefinition() string {
	if c == nil {
		return ""
	}
	return fmt.Sprintf(`"%v" %v`, EscapeString(c.name), EscapeString(c._type))
}

func (c *Argument) getArgumentTypeDefinition() string {
	if c == nil {
		return ""
	}
	return fmt.Sprintf(`%v`, EscapeString(c._type))
}

type Arguments []Argument

func (args Arguments) getArgumentDefinitions() string {
	// TODO(el): verify Snowflake reflects column order back in desc table calls
	definitions := []string{}
	for _, column := range args {
		definitions = append(definitions, column.getArgumentDefinition())
	}

	// NOTE: intentionally blank leading space
	return fmt.Sprintf("(%s)", strings.Join(definitions, ", "))
}

func (args Arguments) getArgumentTypesDefinitions() string {
	// TODO(el): verify Snowflake reflects column order back in desc table calls
	definitions := []string{}
	for _, column := range args {
		definitions = append(definitions, column.getArgumentTypeDefinition())
	}

	// NOTE: intentionally blank leading space
	return fmt.Sprintf("(%s)", strings.Join(definitions, ", "))
}

// Arguments

// UdfBuilder abstracts the creation of SQL queries for a Snowflake UDF
type UdfBuilder struct {
	name       string
	db         string
	schema     string
	replace    bool
	secure     bool
	language   string
	returnType string
	arguments  Arguments
	body       string
}

// QualifiedName prepends the db and schema if set and escapes everything nicely
func (vb *UdfBuilder) QualifiedName() (string, error) {
	if vb.db == "" || vb.schema == "" {
		return "", errors.New("Functions must specify a database and a schema")
	}

	return fmt.Sprintf(`"%v"."%v"."%v"`, vb.db, vb.schema, vb.name), nil
}

// WithLanguage adds a comment to the UdfBuilder
func (vb *UdfBuilder) WithLanguage(c string) *UdfBuilder {
	vb.language = c
	return vb
}

// WithDB adds the name of the database to the UdfBuilder
func (vb *UdfBuilder) WithDB(db string) *UdfBuilder {
	vb.db = db
	return vb
}

// WithArguments sets the column definitions on the UdfBuilder
func (ub *UdfBuilder) WithArguments(args Arguments) *UdfBuilder {
	ub.arguments = args
	return ub
}

// WithReplace adds the "OR REPLACE" option to the UdfBuilder
func (vb *UdfBuilder) WithReplace() *UdfBuilder {
	vb.replace = true
	return vb
}

// WithSecure sets the secure boolean to true
// [Snowflake Reference](https://docs.snowflake.net/manuals/user-guide/views-secure.html)
func (vb *UdfBuilder) WithSecure() *UdfBuilder {
	vb.secure = true
	return vb
}

// WithSchema adds the name of the schema to the UdfBuilder
func (vb *UdfBuilder) WithSchema(s string) *UdfBuilder {
	vb.schema = s
	return vb
}

// WithReturnType adds the name of the schema to the UdfBuilder
func (vb *UdfBuilder) WithReturnType(s string) *UdfBuilder {
	vb.returnType = s
	return vb
}

// WithBody adds the SQL body to be used for the function
func (vb *UdfBuilder) WithBody(s string) *UdfBuilder {
	vb.body = s
	return vb
}

// Function returns a pointer to a Builder that abstracts the DDL operations for a function.
//
// Supported DDL operations are:
//   - CREATE FUNCTION
//   - ALTER FUNCTION
//   - DROP FUNCTION
//   - SHOW FUNCTIONS
//   - DESCRIBE FUNCTION
//
// [Snowflake Reference](https://docs.snowflake.net/manuals/sql-reference/ddl-table.html#standard-view-management)
func Udf(name string) *UdfBuilder {
	return &UdfBuilder{
		name: name,
	}
}

// Create returns the SQL query that will create a new udf.
func (vb *UdfBuilder) Create() (string, error) {
	var q strings.Builder

	q.WriteString("CREATE")

	if vb.replace {
		q.WriteString(" OR REPLACE")
	}

	if vb.secure {
		q.WriteString(" SECURE")
	}

	qn, err := vb.QualifiedName()
	if err != nil {
		return "", err
	}

	q.WriteString(fmt.Sprintf(` FUNCTION %v`, qn))

	// Parameters here BEGIN
	q.WriteString(fmt.Sprintf(` %v`, vb.arguments.getArgumentDefinitions()))
	// Parameters here END

	if vb.returnType != "" {
		q.WriteString(fmt.Sprintf(" RETURNS %v", EscapeString(vb.returnType)))
	}

	if vb.language != "" {
		q.WriteString(fmt.Sprintf(" LANGUAGE %v", EscapeString(vb.language)))
	}

	q.WriteString(fmt.Sprintf(" AS $$ %v $$", vb.body))

	return q.String(), nil
}

// Rename returns the SQL query that will rename the udf.
func (vb *UdfBuilder) Rename(newName string) (string, error) {
	oldName, err := vb.QualifiedName()
	if err != nil {
		return "", err
	}
	vb.name = newName

	qn, err := vb.QualifiedName()
	if err != nil {
		return "", err
	}

	dataTypes := vb.arguments.getArgumentTypesDefinitions()

	return fmt.Sprintf(`ALTER FUNCTION %v %v RENAME TO %v`, oldName, dataTypes, qn), nil
}

// Secure returns the SQL query that will change the view to a secure view.
func (vb *UdfBuilder) Secure() (string, error) {
	qn, err := vb.QualifiedName()
	if err != nil {
		return "", err
	}

	dataTypes := vb.arguments.getArgumentTypesDefinitions()

	return fmt.Sprintf(`ALTER FUNCTION %v %v SET SECURE`, qn, dataTypes), nil
}

// Unsecure returns the SQL query that will change the view to a normal (unsecured) function.
func (vb *UdfBuilder) Unsecure() (string, error) {
	qn, err := vb.QualifiedName()
	if err != nil {
		return "", err
	}

	dataTypes := vb.arguments.getArgumentTypesDefinitions()

	return fmt.Sprintf(`ALTER FUNCTION %v %v UNSET SECURE`, qn, dataTypes), nil
}

// // ChangeComment returns the SQL query that will update the comment on the view.
// // Note that comment is the only parameter, if more are released this should be
// // abstracted as per the generic builder.
// func (vb *ViewBuilder) ChangeComment(c string) (string, error) {
// 	qn, err := vb.QualifiedName()
// 	if err != nil {
// 		return "", err
// 	}

// 	return fmt.Sprintf(`ALTER VIEW %v SET COMMENT = '%v'`, qn, EscapeString(c)), nil
// }

// // RemoveComment returns the SQL query that will remove the comment on the view.
// // Note that comment is the only parameter, if more are released this should be
// // abstracted as per the generic builder.
// func (vb *ViewBuilder) RemoveComment() (string, error) {
// 	qn, err := vb.QualifiedName()
// 	if err != nil {
// 		return "", err
// 	}
// 	return fmt.Sprintf(`ALTER VIEW %v UNSET COMMENT`, qn), nil
// }

// Show returns the SQL query that will show the row representing this udf.
func (vb *UdfBuilder) Show() string {
	return fmt.Sprintf(`SHOW FUNCTIONS LIKE '%v' IN SCHEMA "%v"."%v"`, vb.name, vb.db, vb.schema)
}

// Drop returns the SQL query that will drop the row representing this udf.
func (vb *UdfBuilder) Drop() (string, error) {
	qn, err := vb.QualifiedName()
	if err != nil {
		return "", err
	}

	dataTypes := vb.arguments.getArgumentTypesDefinitions()

	return fmt.Sprintf(`DROP FUNCTION %v %v`, qn, dataTypes), nil
}

type udf struct {
	Comment      sql.NullString `db:"comment"`
	IsSecure     bool           `db:"is_secure"`
	Name         sql.NullString `db:"name"`
	SchemaName   sql.NullString `db:"schema_name"`
	Language     sql.NullString `db:"language"`
	Arguments    sql.NullString `db:"arguments"`
	DatabaseName sql.NullString `db:"database_name"`
}

func ScanUdf(row *sqlx.Row) (*udf, error) {
	r := &udf{}
	err := row.StructScan(r)
	return r, err
}
