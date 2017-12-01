package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"

	"github.com/linkedin/goavro"
)

var (
	codec *goavro.Codec
)

func init() {

	schema, err := ioutil.ReadFile("schema.avsc")
	if err != nil {
		panic(err)
	}

	//Create Schema Once
	codec, err = goavro.NewCodec(string(schema))
	if err != nil {
		panic(err)
	}
}

func main() {

	//Sample Data
	user := &User{
		FirstName: "John",
		LastName:  "Snow",
		Address: &Address{
			Address1: "1106 Pennsylvania Avenue",
			City:     "Wilmington",
			State:    "DE",
			Zip:      19806,
		},
	}

	fmt.Printf("user in=%+v\n", user)

	///Convert Binary From Native
	binary, err := codec.BinaryFromNative(nil, user.ToStringMap())
	if err != nil {
		panic(err)
	}

	///Convert Native from Binary
	native, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		panic(err)
	}

	//Convert it back tp Native
	userOut := StringMapToUser(native.(map[string]interface{}))
	fmt.Printf("user out=%+v\n", userOut)
	if ok := reflect.DeepEqual(user, userOut); !ok {
		fmt.Fprintf(os.Stderr, "struct Compare Failed ok=%t\n", ok)
		os.Exit(1)
	}
}

// User holds information about a user.
type User struct {
	FirstName string
	LastName  string
	Errors    []string
	Address   *Address
}

// Address holds information about an address.
type Address struct {
	Address1 string
	Address2 string
	City     string
	State    string
	Zip      int
}

// ToStringMap returns a map representation of the User.
func (u *User) ToStringMap() map[string]interface{} {
	datumIn := map[string]interface{}{
		"FirstName": string(u.FirstName),
		"LastName":  string(u.LastName),
	}

	if len(u.Errors) > 0 {
		datumIn["Errors"] = goavro.Union("array", u.Errors)
	} else {
		datumIn["Errors"] = goavro.Union("null", nil)
	}

	if u.Address != nil {
		addDatum := map[string]interface{}{
			"Address1": string(u.Address.Address1),
			"City":     string(u.Address.City),
			"State":    string(u.Address.State),
			"Zip":      int(u.Address.Zip),
		}
		if u.Address.Address2 != "" {
			addDatum["Address2"] = goavro.Union("string", u.Address.Address2)
		} else {
			addDatum["Address2"] = goavro.Union("null", nil)
		}

		//important need namespace and record name
		datumIn["Address"] = goavro.Union("my.namespace.com.address", addDatum)

	} else {
		datumIn["Address"] = goavro.Union("null", nil)
	}
	return datumIn
}

// StringMapToUser returns a User from a map representation of the User.
func StringMapToUser(data map[string]interface{}) *User {

	ind := &User{}
	for k, v := range data {
		switch k {
		case "FirstName":
			if value, ok := v.(string); ok {
				ind.FirstName = value
			}
		case "LastName":
			if value, ok := v.(string); ok {
				ind.LastName = value
			}
		case "Errors":
			if value, ok := v.(map[string]interface{}); ok {
				for _, item := range value["array"].([]interface{}) {
					ind.Errors = append(ind.Errors, item.(string))
				}
			}
		case "Address":
			if vmap, ok := v.(map[string]interface{}); ok {
				//important need namespace and record name
				if cookieSMap, ok := vmap["my.namespace.com.address"].(map[string]interface{}); ok {
					add := &Address{}
					for k, v := range cookieSMap {
						switch k {
						case "Address1":
							if value, ok := v.(string); ok {
								add.Address1 = value
							}
						case "Address2":
							if value, ok := v.(string); ok {
								add.Address2 = value
							}
						case "City":
							if value, ok := v.(string); ok {
								add.City = value
							}
						case "Zip":
							if value, ok := v.(int); ok {
								add.Zip = value
							}
						}
					}
					ind.Address = add
				}
			}
		}

	}
	return ind

}
