package main

import (
	"github.com/alessiosavi/GoWarehouseValidator/datastructure"
	"bytes"
	"flag"
	"fmt"
	csvutils "github.com/alessiosavi/GoGPUtils/csv"
	fileutils "github.com/alessiosavi/GoGPUtils/files"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func main() {

	log.SetFlags(log.Ldate | log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	// Load the configuration file
	cfg := flagParser()

	// Initialize a new validator from the configuration file
	validator := datastructure.NewValidatorFromFile(cfg)
	// Measure the time execution of the process

	// Iterate the file that have to be loaded from s3/filesystem

	for _, toValidate := range validator.Conf.Conf {
		for _, f := range toValidate.Path {
			var sb strings.Builder
			log.Println("Validating file [" + f + "]")
			// Read the file and load into a buffered scanner
			file, err := validator.LoadFile(f)
			if err != nil {
				log.Println("ERROR! |", err.Error())
				continue
			}
			defer file.Close()
			start := time.Now()
			data, err := ioutil.ReadAll(file)
			if err != nil {
				panic(err)
			}
			csvHeaders, csvData, err := csvutils.ReadCSV(data, stringutils.GetFirstRune(toValidate.Separator))

			// Split the header of the csv using the input separator
			if len(csvHeaders) != len(toValidate.Validation) {
				panic("Headers line have different length")
			}
			// Remove the UTF-8 BOM from the first line of the CSV
			if bytes.HasPrefix([]byte(csvHeaders[0]), datastructure.BOM) {
				csvHeaders[0] = string(bytes.Replace([]byte(csvHeaders[0]), datastructure.BOM, []byte(""), 1))
			}
			// Iterate the key of the validation map
			for key := range toValidate.Validation {
				// Verify the that the given map key is present in the csv headers
				if !stringutils.CheckPresence(csvHeaders, key) {
					panic(fmt.Sprintf("headers %s does not contains the following header: [%s]", csvHeaders, key))
				}
			}

			// Iterate every line of the (buffered) file
			var rowN int = 1
			for _, lines := range csvData {
				// Number of the row analyzed, just for easy debug in case of error
				rowN++
				// Retrieve every field of the row
				// Validate the length of the field against the length of the csv header
				if len(lines) != len(csvHeaders) {
					log.Println("Seems that the number of field are not the same of the configuration for the file " + f)
					fmt.Println(fmt.Sprintf("Error on line %d", rowN))
				}
				// Iterating the headers of the csv and the the lines of the row
				// 	key = name of the headers of the csv
				//	field = field of the csv row
				for i, key := range csvHeaders {
					field := lines[i]
					validationType := toValidate.Validation[key]

					if strings.HasSuffix(validationType, "|NULLABLE") {
						// Ignore the data that can be null
						if stringutils.IsBlank(field) {
							continue
						}
						validationType = validationType[:strings.Index(validationType, "|")]
					}
					switch validationType {
					case "INTEGER":
						_, err = strconv.Atoi(field)
						if err != nil {
							sb.WriteString(fmt.Sprintf("%s,%d] Field [%s] is !NOT! an INTEGER\n", key, rowN, field))
						}
					case "DATE":
						_, err = time.Parse(toValidate.DateFormat, field)
						if err != nil {
							sb.WriteString(fmt.Sprintf("%s,%d] Field [%s] is !NOT! a DATE | Error: %s\n", key, rowN, field, err.Error()))
						}
					case "STRING":
						if stringutils.IsBlank(field) || !utf8.ValidString(field) {
							sb.WriteString(fmt.Sprintf("%s,%d] Field [%s] is !NOT! a STRING\n", key, rowN, field))
						}
						count := strings.Count(field, `"`)
						if count > 0 && count != 2 {
							sb.WriteString(fmt.Sprintf("%s,%d] Field [%s] contains a number of \" different from 2!\n", key, rowN, field))
						}
					case "FLOAT":
						_, err = strconv.ParseFloat(field, 64)
						if err != nil {
							sb.WriteString(fmt.Sprintf("%s] Field [%s] is !NOT! a FLOAT\n", key, field))
						}
					default:
						log.Printf("Condition [" + toValidate.Validation[key] + "] not managed")
					}
				}
			}
			filename := path.Base(f)
			if sb.Len() > 0 {
				basepath := path.Dir(strings.Replace(f, "s3://", "", 1))

				if err = os.MkdirAll(basepath, 0755); err != nil {
					log.Println("Unable to dump the result:", err)
				} else {
					if err = ioutil.WriteFile(path.Join(basepath, filename), []byte(sb.String()), 0755); err != nil {
						log.Println("Unable to dump the result:", err)
					}
					log.Printf("Saving result in: %s", path.Join(basepath, filename))
				}
				if err = file.Close(); err != nil {
					panic("Unable to close file: " + f)
				}
			} else {
				log.Printf("File %s is valid!\n", f)
			}
			duration := time.Since(start)
			log.Printf("Validating file [%s] took: %+v\n ", filename, duration)

		}
	}

}

// flagParser is delegated to retrieve the input configuration
func flagParser() string {
	cfg := flag.String("conf", "", "path related to the json configuration file")
	flag.Parse()
	if stringutils.IsBlank(*cfg) {
		flag.Usage()
		panic("-conf parameter not provided!")
	}
	if !fileutils.FileExists(*cfg) {
		panic("configuration file `" + *cfg + "` not found!")
	}
	return *cfg
}
