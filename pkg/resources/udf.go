package resources

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/pkg/errors"
)

var udfSpace = regexp.MustCompile(`\s+`)

var udfSchema = map[string]*schema.Schema{
	"name": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "Specifies the identifier for the function; must be unique, in combination with parameters, for the schema in which the function is created. Don't use the | character.",
	},
	"database": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "The database in which to create the function. Don't use the | character.",
		ForceNew:    true,
	},
	"schema": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "The schema in which to create the function. Don't use the | character.",
		ForceNew:    true,
	},
	"or_replace": {
		Type:        schema.TypeBool,
		Optional:    true,
		Default:     false,
		Description: "Overwrites the function if it exists.",
	},
	"is_secure": {
		Type:        schema.TypeBool,
		Optional:    true,
		Default:     false,
		Description: "Specifies that the function is secure.",
	},
	"return_type": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "Specifies return type of the udf.",
	},
	"language": {
		Type:        schema.TypeString,
		Optional:    true,
		Default:     false,
		Description: "Specifies the language used in the body of the udf.",
	},
	"argument": {
		Type:        schema.TypeList,
		Required:    true,
		MinItems:    0,
		ForceNew:    true,
		Description: "Definitions of an argument the function is able to receive.",
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"name": {
					Type:        schema.TypeString,
					Required:    true,
					Description: "Argument name",
				},
				"type": {
					Type:        schema.TypeString,
					Required:    true,
					Description: "Argument type, e.g. VARIANT",
				},
			},
		},
	},
	"body": {
		Type:             schema.TypeString,
		Required:         true,
		Description:      "Specifies the query used to create the function.",
		ForceNew:         true,
		DiffSuppressFunc: udfBodyStatementDiffSuppress,
	},
}

// func udfNormalizeQuery(str string) string {
// 	return strings.TrimSpace(udfSpace.ReplaceAllString(str, " "))
// }

// DiffSuppressBody will suppress diffs between statemens if they differ in only case or in
// runs of whitespace (\s+ = \s). This is needed because the snowflake api does not faithfully
// round-trip queries so we cannot do a simple character-wise comparison to detect changes.
//
// Warnings: We will have false positives in cases where a change in case or run of whitespace is
// semantically significant.
//
// If we can find a sql parser that can handle the snowflake dialect then we should switch to parsing
// queries and either comparing ASTs or emiting a canonical serialization for comparison. I couldn't
// find such a library.
func udfBodyStatementDiffSuppress(_, old, new string, d *schema.ResourceData) bool {
	// standardise line endings
	old = strings.ReplaceAll(old, "\r\n", "\n")
	new = strings.ReplaceAll(new, "\r\n", "\n")

	return strings.TrimRight(old, ";\r\n") == strings.TrimRight(new, ";\r\n")
}

// Udf id should be made up by the full function signature, in this first version I am supporting only the name of the function
type udfID struct {
	DatabaseName string
	SchemaName   string
	Name         string
}

//String() takes in a udfID object and returns a pipe-delimited string:
//DatabaseName|SchemaName|Name
func (si *udfID) String() (string, error) {
	var buf bytes.Buffer
	csvWriter := csv.NewWriter(&buf)
	csvWriter.Comma = '|'
	dataIdentifiers := [][]string{{si.DatabaseName, si.SchemaName, si.Name}}
	err := csvWriter.WriteAll(dataIdentifiers)
	if err != nil {
		return "", err
	}
	strUdfID := strings.TrimSpace(buf.String())
	return strUdfID, nil
}

// udfIDFromString() takes in a pipe-delimited string: DatabaseName|SchemaName|Name
// and returns a udfID object
func udfIDFromString(stringID string) (*udfID, error) {
	reader := csv.NewReader(strings.NewReader(stringID))
	reader.Comma = pipeIDDelimiter
	lines, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("Not CSV compatible")
	}

	if len(lines) != 1 {
		return nil, fmt.Errorf("1 line per pipe")
	}
	if len(lines[0]) != 3 {
		return nil, fmt.Errorf("3 fields allowed")
	}

	udfResult := &udfID{
		DatabaseName: lines[0][0],
		SchemaName:   lines[0][1],
		Name:         lines[0][2],
	}
	return udfResult, nil
}

// Udf returns a pointer to the resource representing an udf
func Udf() *schema.Resource {
	return &schema.Resource{
		Create: CreateUdf,
		Read:   ReadUdf,
		Update: UpdateUdf,
		Delete: DeleteUdf,

		Schema: udfSchema,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

// CreateUdf implements schema.CreateFunc
func CreateUdf(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Get("name").(string)
	schema := d.Get("schema").(string)
	database := d.Get("database").(string)
	b := d.Get("body").(string)

	arguments := []snowflake.Argument{}
	// argumentTypes := []string{}

	for _, argument := range d.Get("argument").([]interface{}) {
		typed := argument.(map[string]interface{})
		argDef := snowflake.Argument{}
		argDef.WithName(typed["name"].(string)).WithType(typed["type"].(string))
		arguments = append(arguments, argDef)
		// argumentTypes = append(argumentTypes, typed["type"].(string))
	}

	builder := snowflake.Udf(name).WithDB(database).WithSchema(schema).WithBody(b).WithArguments(arguments)

	// Set optionals
	if v, ok := d.GetOk("or_replace"); ok && v.(bool) {
		builder.WithReplace()
	}

	if v, ok := d.GetOk("is_secure"); ok && v.(bool) {
		builder.WithSecure()
	}

	if v, ok := d.GetOk("return_type"); ok {
		builder.WithReturnType(v.(string))
	}

	if v, ok := d.GetOk("language"); ok {
		builder.WithLanguage(v.(string))
	}

	q, err := builder.Create()
	if err != nil {
		return err
	}

	err = snowflake.Exec(db, q)
	if err != nil {
		return errors.Wrapf(err, "error creating Udf %v", name)
	}

	// On Snowflake functions can be overloaded, hence the actual identifier is given by the name, plus the list of argument types
	// d.SetId(fmt.Sprintf("%v|%v|%v(%v)", database, schema, name, strings.Join(argumentTypes, ", ")))
	udfID := &udfID{
		DatabaseName: database,
		SchemaName:   schema,
		Name:         name,
	}
	dataIDInput, err := udfID.String()
	if err != nil {
		return err
	}
	d.SetId(dataIDInput)

	return ReadUdf(d, meta)
}

// ReadUdf implements schema.ReadFunc
func ReadUdf(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	udfID, err := udfIDFromString(d.Id())
	if err != nil {
		return err
	}

	dbName := udfID.DatabaseName
	schema := udfID.SchemaName
	name := udfID.Name

	q := snowflake.Udf(name).WithDB(dbName).WithSchema(schema).Show()
	row := snowflake.QueryRow(db, q)
	v, err := snowflake.ScanUdf(row)
	if err == sql.ErrNoRows {
		// If not found, mark resource to be removed from statefile during apply or refresh
		log.Printf("[DEBUG] Udf (%s) not found", d.Id())
		d.SetId("")
		return nil
	}
	if err != nil {
		return err
	}

	err = d.Set("name", v.Name.String)
	if err != nil {
		return err
	}

	err = d.Set("is_secure", v.IsSecure)
	if err != nil {
		return err
	}

	err = d.Set("language", v.Language.String)
	if err != nil {
		return err
	}

	// Check this from table
	// err = d.Set("arguments", v.Arguments.String)
	// if err != nil {
	// 	return err
	// }

	err = d.Set("schema", v.SchemaName.String)
	if err != nil {
		return err
	}

	// Want to only capture the Select part of the query because before that is the Create part of the Udf which we no longer care about

	// extractor := snowflake.NewUdfSelectStatementExtractor(v.Text.String)
	// substringOfQuery, err := extractor.Extract()
	// if err != nil {
	// 	return err
	// }

	// err = d.Set("statement", substringOfQuery)
	// if err != nil {
	// 	return err
	// }

	return d.Set("database", v.DatabaseName.String)
}

// UpdateUdf implements schema.UpdateFunc
//  TODO
func UpdateUdf(d *schema.ResourceData, meta interface{}) error {
	udfID, err := udfIDFromString(d.Id())
	if err != nil {
		return err
	}

	dbName := udfID.DatabaseName
	schema := udfID.SchemaName
	name := udfID.Name

	builder := snowflake.Udf(name).WithDB(dbName).WithSchema(schema)

	db := meta.(*sql.DB)
	if d.HasChange("name") {
		name := d.Get("name")

		q, err := builder.Rename(name.(string))
		if err != nil {
			return err
		}
		err = snowflake.Exec(db, q)
		if err != nil {
			return errors.Wrapf(err, "error renaming Udf %v", d.Id())
		}

		d.SetId(fmt.Sprintf("%v|%v|%v", dbName, schema, name.(string)))
	}

	// if d.HasChange("comment") {
	// 	comment := d.Get("comment")

	// 	if c := comment.(string); c == "" {
	// 		q, err := builder.RemoveComment()
	// 		if err != nil {
	// 			return err
	// 		}
	// 		err = snowflake.Exec(db, q)
	// 		if err != nil {
	// 			return errors.Wrapf(err, "error unsetting comment for Udf %v", d.Id())
	// 		}
	// 	} else {
	// 		q, err := builder.ChangeComment(c)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		err = snowflake.Exec(db, q)
	// 		if err != nil {
	// 			return errors.Wrapf(err, "error updating comment for Udf %v", d.Id())
	// 		}
	// 	}
	// }
	if d.HasChange("is_secure") {
		secure := d.Get("is_secure")

		if secure.(bool) {
			q, err := builder.Secure()
			if err != nil {
				return err
			}
			err = snowflake.Exec(db, q)
			if err != nil {
				return errors.Wrapf(err, "error setting secure for Udf %v", d.Id())
			}
		} else {
			q, err := builder.Unsecure()
			if err != nil {
				return err
			}
			err = snowflake.Exec(db, q)
			if err != nil {
				return errors.Wrapf(err, "error unsetting secure for Udf %v", d.Id())
			}
		}
	}

	return ReadUdf(d, meta)
}

// DeleteUdf implements schema.DeleteFunc
func DeleteUdf(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	udfID, err := udfIDFromString(d.Id())
	if err != nil {
		return err
	}

	dbName := udfID.DatabaseName
	schema := udfID.SchemaName
	name := udfID.Name

	q, err := snowflake.Udf(name).WithDB(dbName).WithSchema(schema).Drop()
	if err != nil {
		return err
	}

	err = snowflake.Exec(db, q)
	if err != nil {
		return errors.Wrapf(err, "error deleting Udf %v", d.Id())
	}

	d.SetId("")

	return nil
}

// UdfExists implements schema.ExistsFunc
func UdfExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	db := meta.(*sql.DB)
	udfID, err := udfIDFromString(d.Id())
	if err != nil {
		return false, err
	}

	dbName := udfID.DatabaseName
	schema := udfID.SchemaName
	name := udfID.Name

	q := snowflake.Udf(name).WithDB(dbName).WithSchema(schema).Show()
	rows, err := db.Query(q)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		return true, nil
	}

	return false, nil
}
