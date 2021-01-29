package main

import (
	"GoWarehouseValidator/datastructure"
	"bufio"
	"bytes"
	"flag"
	"fmt"
	fileutils "github.com/alessiosavi/GoGPUtils/files"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"log"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var bom = []byte{0xef, 0xbb, 0xbf} // UTF-8
func main() {
	var lines []string
	var err error

	log.SetFlags(log.Ldate | log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	// Load the configuration file
	cfg := flagParser()

	// Initialize a new validator from the configuration file
	validator := datastructure.NewValidatorFromConf(cfg)
	// Measure the time execution of the process
	start := time.Now()

	var sb strings.Builder

	// Iterate the file that have to be loaded from s3/filesystem

	for _, toValidate := range validator.Conf.Conf {
		for _, f := range toValidate.Path {
			log.Println("Validating file [" + f + "]")
			// Read the file and load into a buffered scanner
			file := validator.LoadFile(f)
			defer file.Close()
			scanner := bufio.NewScanner(file)
			// Get the header of the CSV. These headers have to be validated against key of the map in the `validation`
			//  field from the configuration json
			scanner.Scan()
			// Split the header of the csv using the input separator
			csvHeaders := strings.Split(scanner.Text(), toValidate.Separator)
			if len(csvHeaders) != len(toValidate.Validation) {
				panic("Headers line have different length")
			}
			// Remove the UTF-8 BOM from the first line of the CSV
			if bytes.HasPrefix([]byte(csvHeaders[0]), bom) {
				csvHeaders[0] = string(bytes.Replace([]byte(csvHeaders[0]), bom, []byte(""), 1))
			}
			// Iterate the key of the validation map
			for key := range toValidate.Validation {
				// Verify the that the given map key is present in the csv headers
				if !stringutils.CheckPresence(csvHeaders, key) {
					panic(fmt.Sprintf("headers %s does not contains the following header: [%s]", csvHeaders, key))
				}
			}

			// Iterate every line of the (buffered) file
			var row int = 1
			for scanner.Scan() {
				// Number of the row analyzed, just for easy debug in case of error
				row++
				// Retrieve every field of the row
				lines = strings.Split(scanner.Text(), toValidate.Separator)
				// Validate the length of the field against the length of the csv header
				if len(lines) != len(csvHeaders) {
					log.Println("Seems that the number of field are not the same of the configuration")
					fmt.Println(fmt.Sprintf("Error on line %d", row))
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
							sb.WriteString(fmt.Sprintf("%s,%d] Field [%s] is !NOT! an INTEGER\n", key, row, field))
						}
					case "DATE":
						_, err = time.Parse(toValidate.DateFormat, field)
						if err != nil {
							sb.WriteString(fmt.Sprintf("%s,%d] Field [%s] is !NOT! a DATE | Error: %s\n", key, row, field, err.Error()))
						}
					case "STRING":
						if stringutils.IsBlank(field) || !utf8.ValidString(field) {
							sb.WriteString(fmt.Sprintf("%s,%d] Field [%s] is !NOT! a STRING\n", key, row, field))
						}
						count := strings.Count(field, `"`)
						if count > 0 && count != 2 {
							sb.WriteString(fmt.Sprintf("%s,%d] Field [%s] contains a number of \" different from 2!\n", key, row, field))
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

			if err = file.Close(); err != nil {
				panic("Unable to close file: " + f)
			}
		}
	}

	duration := time.Since(start)
	if sb.Len() > 0 {
		log.Println(sb.String())
	}
	log.Println(duration)
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
