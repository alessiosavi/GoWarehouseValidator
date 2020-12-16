package datastructure

import (
	"encoding/json"
	"fmt"
	fileutils "github.com/alessiosavi/GoGPUtils/files"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"os"
	. "strings"
)

type Conf struct {
	// Path related to the file (s3 or filesystem)
	Path       []string          `json:"path"`
	Validation map[string]string `json:"validation"`
	Region     string            `json:"region"`
	Separator  string            `json:"separator"`
	DateFormat string            `json:"date_format"`
}

type Validator struct {
	Conf      Conf
	S3session *s3.S3
}

func (c *Conf) Validate() {
	if c == nil {
		panic("configuration is null")
	}
	if len(c.Path) == 0 {
		panic("empty path in configuration")
	}

	if c.Validation == nil || len(c.Validation) == 0 {
		panic("validation map is empty!")
	}
	if stringutils.IsBlank(c.Region) {
		panic("empty region for s3 bucket")
	}
	if stringutils.IsBlank(c.Separator) {
		panic("separator not provided")
	}
	if stringutils.IsBlank(c.DateFormat) {
		panic("date format not provided")
	}
}
func (v *Validator) LoadFile(path string) io.ReadCloser {
	// Load file from S3
	if HasPrefix(path, "s3://") {
		totalPath := path[len("s3://"):]
		lastIndex := LastIndex(totalPath, "/")
		fName := totalPath[lastIndex:]
		firstIndex := Index(totalPath, "/")
		bucketName := totalPath[:firstIndex]
		object, err := v.S3session.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fName),
		})
		if err != nil {
			panic(err)
		}
		return object.Body

	} else if fileutils.FileExists(path) {
		open, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		return open
	}
	return nil
}

func createStrfTimeMap(dateformat string) map[string]string {
	var strf map[string]string = make(map[string]string)
	replacer := NewReplacer("/", "", " ", "", "-", "", ":", "", ",", "", ".", "")
	dateformat = replacer.Replace(dateformat)
	for _, format := range Split(dateformat, "%") {
		if len(format) == 0 {
			continue
		}
		format = "%" + string(format[0])
		switch format {
		case "%a":
			strf["%a"] = `Mon`
		case "%A":
			strf["%A"] = "Monday "
		case "%b":
			strf["%b"] = "Jan"
		case "%B":
			strf["%B"] = "January"
		//case "%c":
		//	strf["%c"] = ""
		//case "%C":
		//	strf["%C"] = ""
		case "%d":
			strf["%d"] = "02"
		//case "%D":
		//	strf["%D"] = ""
		//case "%e":
		//	strf["%e"] = ""
		//case "%F":
		//	strf["%F"] = ""
		//case "%g":
		//	strf["%g"] = ""
		//case "%G":
		//	strf["%G"] = ""
		case "%h":
			strf["%h"] = "Jan"
		case "%H":
			strf["%H"] = "15"
		case "%I":
			strf["%I"] = "03"
		//case "%j":
		//	strf["%j"] = ""
		case "%m":
			strf["%m"] = "01"
		case "%M":
			strf["%M"] = "04"
		//case "%p":
		//	strf["%p"] = ""
		//case "%r":
		//	strf["%r"] = ""
		case "%R":
			strf["%R"] = "15:04"
		case "%S":
			strf["%S"] = "05"
		//case "%t":
		//	strf["%t"] = ""
		case "%T":
			strf["%T"] = "15:04:05"
		//case "%u":
		//	strf["%u"] = ""
		//case "%U":
		//	strf["%U"] = ""
		//case "%V":
		//	strf["%V"] = ""
		//case "%w":
		//	strf["%w"] = ""
		//case "%W":
		//	strf["%W"] = ""
		//case "%x":
		//	strf["%c"] = ""
		case "%X":
			strf["%X"] = "15:04:05"
		case "%y":
			strf["%y"] = "06"
		case "%Y":
			strf["%Y"] = "2006"
		//case "%z":
		//	strf["%z"] = ""
		//case "%Z":
		//	strf["%Z"] = ""
		default:
			if !stringutils.IsBlank(format) {
				panic(fmt.Sprintf("format [%s] not found!", format))
			}
		}
	}
	return strf
}

func (c *Conf) SetDateFormat() {
	timeMap := createStrfTimeMap(c.DateFormat)
	for key := range timeMap {
		c.DateFormat = ReplaceAll(c.DateFormat, key, timeMap[key])
	}
}

// NewValidator is delegated to verify if the given configuration is valid, then initialize a new validator object.
//	This object will take in care the validation of the various dataset specified in configuration.
func (c *Conf) NewValidator() *Validator {
	indent, err := json.MarshalIndent(c, " ", "  ")
	if err != nil {
		panic(err)
	}
	log.Println("Using the following configuration:")
	log.Println(string(indent))
	// Validate the configuration
	c.Validate()
	//sess, err := session.NewSession()
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String(ToLower(c.Region))},
	})
	if err != nil {
		panic(err)
	}
	var v Validator
	v.S3session = s3.New(sess)
	v.Conf = *c
	v.Conf.SetDateFormat()
	return &v
}