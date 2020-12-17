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
)

var bom = []byte{0xef, 0xbb, 0xbf} // UTF-8
func main() {
	var lines []string
	var row int

	log.SetFlags(log.Ldate | log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	// Load the configuration file
	cfg := flagParser()
	// Initialize a new validator from the configuration file
	validator := datastructure.NewValidatorFromConf(cfg)
	// Mesaure the time execution of the process
	start := time.Now()
	// Iterate the file that have to be loaded from s3/filesystem
	for _, f := range validator.Conf.Path {
		log.Println("Validating file [" + f + "]")
		// Read the file and load into a buffered scanner
		file := validator.LoadFile(f)
		scanner := bufio.NewScanner(file)
		// Get the header of the CSV. These headers have to be validated against key of the map in the `validation`
		//  field from the configuration json
		scanner.Scan()
		// Split the header of the csv using the input separator
		csvHeaders := strings.Split(scanner.Text(), validator.Conf.Separator)
		if len(csvHeaders) != len(validator.Conf.Validation) {
			panic("Headers line have different length")
		}
		// Remove the UTF-8 BOM from the first line of the CSV
		if bytes.HasPrefix([]byte(csvHeaders[0]), bom) {
			csvHeaders[0] = string(bytes.Replace([]byte(csvHeaders[0]), bom, []byte(""), 1))
		}
		// Iterate the key of the validation map
		for key := range validator.Conf.Validation {
			// Verify the that the given map key is present in the csv headers
			if !stringutils.CheckPresence(csvHeaders, key) {
				panic(fmt.Sprintf("headers %s does not contains the following header: [%s]", csvHeaders, key))
			}
		}

		// Iterate every line of the (buffered) file
		for scanner.Scan() {
			// Retrieve every field of the row
			lines = strings.Split(scanner.Text(), validator.Conf.Separator)
			// Validate the length of the field against the length of the csv header
			if len(lines) != len(csvHeaders) {
				panic(fmt.Sprintf("Error on line %d", row))
			}
			// Iterating the headers of the csv and the the lines of the row
			// 	key = name of the headers of the csv
			//	field = field of the csv row
			for i, key := range csvHeaders {
				field := lines[i]
				validationType := validator.Conf.Validation[key]

				if strings.HasSuffix(validator.Conf.Validation[key], "|NULLABLE") {
					// Ignore the data that can be null
					if stringutils.IsBlank(field) {
						continue
					}
					validationType = validationType[:strings.Index(validationType, "|")]
				}
				switch validationType {
				case "INTEGER":
					_, err := strconv.Atoi(field)
					if err != nil {
						panic(fmt.Sprintf("%s] Field %s is !NOT! an INTEGER\n", key, field))
					}
				case "DATE":
					_, err := time.Parse(validator.Conf.DateFormat, field)
					if err != nil {
						fmt.Println(validator.Conf.DateFormat)
						panic(fmt.Sprintf("%s] Field %s is !NOT! a DATE | \n%s", key, field, err.Error()))
					}
				case "STRING":
					if !stringutils.IsASCII(field) {
						panic(fmt.Sprintf("%s] Field %s is !NOT! a STRING\n", key, field))
					}
				case "FLOAT":
					_, err := strconv.ParseFloat(field, 64)
					if err != nil {
						panic(fmt.Sprintf("%s] Field %s is !NOT! a FLOAT\n", key, field))
					}
				default:
					panic("Condition [" + validator.Conf.Validation[key] + "] not managed")
				}
			}
			// Number of the row analyzed, just for easy debug in case of error
			row++
		}
		err := file.Close()
		if err != nil {
			panic("Unable to close file: " + f)
		}
		log.Println("File [" + f + "] is valid!")
	}

	duration := time.Since(start)
	fmt.Println(duration)
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
