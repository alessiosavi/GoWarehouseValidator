package datastructure

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	fileutils "github.com/alessiosavi/GoGPUtils/files"
	"github.com/alessiosavi/GoGPUtils/helper"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type ValidationConf struct {
	Path       []string          `json:"path"`
	Separator  string            `json:"separator"`
	DateFormat string            `json:"date_format"`
	Validation map[string]string `json:"validation"`
}

type Conf struct {
	Region string           `json:"region"`
	Conf   []ValidationConf `json:"conf"`
}
type Validator struct {
	Conf      Conf
	S3session *s3.Client
}

var BOMS = [][]byte{
	[]byte{0xef, 0xbb, 0xbf},       // UTF-8
	[]byte{0xff, 0xfe},             // UTF-16 LE
	[]byte{0xfe, 0xff},             // UTF-16 BE
	[]byte{0xff, 0xfe, 0x00, 0x00}, // UTF-32 LE
	[]byte{0x00, 0x00, 0xfe, 0xff}, // UTF-32 LE

}

func (conf *Conf) Validate() {
	if conf == nil {
		panic("configuration is null")
	}
	if stringutils.IsBlank(conf.Region) {
		panic("empty region for s3 bucket")
	}
	for _, c := range conf.Conf {
		if len(c.Path) == 0 {
			panic("empty path in configuration")
		}
		if c.Validation == nil || len(c.Validation) == 0 {
			panic("validation map is empty!")
		}
		if stringutils.IsBlank(c.Separator) {
			panic("separator not provided")
		}
		if stringutils.IsBlank(c.DateFormat) {
			panic("date format not provided")
		}
	}
}

func NewValidatorFromFile(cfg string) *Validator {
	var conf Conf
	data, err := ioutil.ReadFile(cfg)
	if err != nil {
		panic(err)
	}
	if err = json.Unmarshal(data, &conf); err != nil {
		panic(err)
	}

	validator := conf.NewValidator()
	return validator
}

func (v *Validator) LoadFile(path string) (io.ReadCloser, error) {
	// Load file from S3
	if strings.HasPrefix(path, "s3://") {
		totalPath := path[len("s3://"):]
		firstIndex := strings.Index(totalPath, "/")
		fName := totalPath[firstIndex+1:]
		bucketName := totalPath[:firstIndex]
		object, err := v.S3session.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fName),
		})
		if err != nil {
			return nil, err
		}
		return object.Body, nil

	} else if fileutils.FileExists(path) && fileutils.IsFile(path) {
		open, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		return open, nil
	}
	return nil, fmt.Errorf("file [%s] does not exist", path)
}

// NewValidator is delegated to verify if the given configuration is valid, then initialize a new validator object.
//	This object will take in care the validation of the various dataset specified in configuration.
func (conf *Conf) NewValidator() *Validator {
	log.Println("Using the following configuration: " + helper.Marshal(conf))
	// Validate the configuration
	conf.Validate()

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}

	var v Validator
	v.S3session = s3.New(s3.Options{Credentials: cfg.Credentials, Region: cfg.Region})
	v.Conf = *conf
	return &v
}

type ErrorLine struct {
	Index     int    `json:"index,omitempty"`
	Row       string `json:"row,omitempty"`
	ErrorType string `json:"error_type,omitempty"`
	FieldName string `json:"field_name,omitempty"`
}

func (v *Validator) ValidateData(headers []string, data [][]string, toValidate ValidationConf) ([]ErrorLine, error) {
	var errorsLine []ErrorLine
	// Split the header of the csv using the input separator
	if len(headers) != len(toValidate.Validation) {
		// If occurs in the first row, than it's probably because the separator configured is different from the one of the file
		return []ErrorLine{{Index: 0, Row: "", ErrorType: "Headers row have different length"}}, nil
	}
	// Remove the various UTF BOM from the first line of the CSV
	for _, bom := range BOMS {
		if bytes.HasPrefix([]byte(headers[0]), bom) {
			headers[0] = string(bytes.Replace([]byte(headers[0]), bom, nil, 1))
		}
	}
	// Iterate the key of the validation map
	for key := range toValidate.Validation {
		// Verify the that the given map key is present in the csv headers
		if !stringutils.CheckPresence(headers, key) {
			return []ErrorLine{{Index: 0, Row: "", ErrorType: fmt.Sprintf("headers %s does not contains the following header: [%s]", headers, key), FieldName: key}}, nil
		}
	}

	// Iterate every row of the csv
	for rowN, row := range data {
		// Retrieve every field of the row
		// Validate the length of the field against the length of the csv header
		if len(row) != len(headers) {
			return nil, errors.New("number of field mismatch with headers")
		}
		// Iterating the headers of the csv and the the row of the row
		// 	key = name of the headers of the csv
		//	field = field of the csv row
		for i, key := range headers {
			field := row[i]
			validationType := toValidate.Validation[key]

			if strings.HasSuffix(validationType, "|NULLABLE") {
				// Ignore the data that can be null
				if stringutils.IsBlank(field) {
					continue
				}
				// Remove nullable from the validation string
				validationType = validationType[:strings.Index(validationType, "|")]
			}
			switch validationType {
			case "INTEGER":
				_, err := strconv.Atoi(field)
				if err != nil {
					errorsLine = append(errorsLine, ErrorLine{Index: rowN, Row: stringutils.JoinSeparator(",", row...),
						ErrorType: fmt.Sprintf("[%s] is not an INTEGER", field), FieldName: key})
				}
			case "DATE":
				_, err := time.Parse(toValidate.DateFormat, field)
				if err != nil {
					errorsLine = append(errorsLine, ErrorLine{Index: rowN, Row: stringutils.JoinSeparator(",", row...),
						ErrorType: fmt.Sprintf("[%s] is not a valid DATE", field), FieldName: key})
				}
			case "STRING":
				if stringutils.IsBlank(field) || !utf8.ValidString(field) {
					errorsLine = append(errorsLine, ErrorLine{Index: rowN, Row: stringutils.JoinSeparator(",", row...),
						ErrorType: fmt.Sprintf("[%s] is not an valid STRING", field), FieldName: key})
				}
				//count := strings.Count(field, `"`)
				//if count > 0 && count != 2 {
				//	sb.WriteString(fmt.Sprintf("%s,%d] Field [%s] contains a number of \" different from 2!\n", key, rowN, field))
				//}
			case "FLOAT":
				if _, err := strconv.ParseFloat(field, 64); err != nil {
					errorsLine = append(errorsLine, ErrorLine{Index: rowN, Row: stringutils.JoinSeparator(",", row...),
						ErrorType: fmt.Sprintf("[%s] is not an INTEGER", field), FieldName: key})
				}
			default:
				log.Printf("Condition [" + toValidate.Validation[key] + "] not managed")
			}

		}
	}
	return errorsLine, nil
}
