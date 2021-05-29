package datastructure

import (
	"github.com/alessiosavi/GoGPUtils/helper"
	"testing"
)

var PricingValidation map[string]string = map[string]string{
	"Channel":              "STRING",
	"Class":                "INTEGER",
	"Class Description":    "STRING",
	"Default":              "STRING",
	"Default2":             "INTEGER",
	"Description":          "STRING",
	"Fabric":               "STRING",
	"Fit Code":             "STRING",
	"Gender":               "STRING",
	"Market":               "STRING",
	"Price":                "FLOAT",
	"Product_ID":           "STRING",
	"Season":               "STRING",
	"Subclass":             "INTEGER|NULLABLE",
	"Subclass Description": "STRING|NULLABLE",
}

func Test_base(t *testing.T) {

	var c Conf = Conf{
		Region: "eu-west-1",
		Conf: []ValidationConf{{
			Path:       []string{"empty"},
			Separator:  ",",
			DateFormat: "1/2/2006",
			Validation: PricingValidation,
		}},
	}
	validator := c.NewValidator()
	headers := []string{"Channel", "Class", "Class Description", "Default", "Default2", "Description", "Fabric", "Fit Code", "Gender", "Market", "Price", "Product_ID", "Season", "Subclass", "Subclass Description"}
	csvData := [][]string{
		{"Wholesale", "0002", "TROUSER", "Seasonal", "1", "SINGLE PLEAT TROUSER - FIT 5 - IN WOOL PIQUE SUITING", "07893", "MTC162A", "MENS", "USA", "440", "MTC162A-07893", "SS22", "129", "FIT 5"},
		{"Wholesale", "0002", "TROUSER", "Seasonal", "1", "SINGLE PLEAT SHORT - FIT 5 - IN WOOL PIQUE SUITING", "07893", "MTC176A", "MENS", "USA", "405", "MTC176A-07893", "SS22", "035", "SHORTS"},
		{"Wholesale", "0003", "JACKET", "Seasonal", "1", "Description", "", "Product", "MENS", "USA", "1175", "Product", "SS22", "126", "UNCONSTR - JACKET"},
		{"Wholesale", "0003", "JACKET", "Seasonal", "1", "joor delete", "07997", "MJU426A", "MENS", "USA", "731", "MJU426A-07997", "SS22", "126", "UNCONSTR - JACKET"},
		{"Wholesale", "0003", "JACKET", "Seasonal", "1", "UNCONSTRUCTED HIGH ARMHOLE SPORT COAT - FIT 3 - W/ 4BAR & FRAY EDGE IN GINGHAM POW SUMMER TWEED", "07888", "MJU547T", "MENS", "USA", "1115", "MJU547T-07888", "SS22", "126", "UNCONSTR - JACKET"},
		{"Wholesale", "0003", "JACKET", "Seasonal", "1", "UNCONSTRUCTED SACK SPORT COAT - FIT 2 - W/ RWB GROSGRAIN PLACKET IN REPP STRIPE WOOL COTTON SUITING", "07849", "MJU505E", "MENS", "USA", "923", "MJU505E-07849", "SS22", "123", "UNCONSTRUCT - JACKET"},
		{"Wholesale", "0002", "TROUSER", "Seasonal", "1", "UNCONSTRUCTED LOW RISE TROUSER - FIT 3 - W/ 4BAR & FRAY EDGE IN GINGHAM POW SUMMER TWEED", "07888", "MTU307T", "MENS", "USA", "765", "MTU307T-07888", "SS22", "031", "FIT 3"},
	}
	data, err := validator.ValidateData(headers, csvData, c.Conf[0])
	if err != nil{
		panic(err)
	}
	t.Log(helper.MarshalIndent(data))

}
