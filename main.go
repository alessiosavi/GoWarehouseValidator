package main

import (
	"GoWarehouseValidator/datastructure"
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	fileutils "github.com/alessiosavi/GoGPUtils/files"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"
)

var bom = []byte{0xef, 0xbb, 0xbf} // UTF-8
func main() {

	log.SetFlags(log.Ldate | log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	cfg := flag.String("conf", "", "path related to the json configuration file")
	flag.Parse()

	if stringutils.IsBlank(*cfg) {
		flag.Usage()
		panic("-conf parameter not provided!")
	}
	if !fileutils.FileExists(*cfg) {
		panic("configuration file `" + *cfg + "` not found!")
	}
	var conf datastructure.Conf

	data, err := ioutil.ReadFile(*cfg)
	if err != nil {
		panic(err)
	}
	if err = json.Unmarshal(data, &conf); err != nil {
		panic(err)
	}

	validator := conf.NewValidator()
	start := time.Now()
	for _, f := range validator.Conf.Path {
		log.Println("Validating file [" + f + "]")
		file := validator.LoadFile(f)
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Scan()
		keys := strings.Split(scanner.Text(), validator.Conf.Separator)
		if len(keys) != len(validator.Conf.Validation) {
			panic("Headers line have different length")
		}
		// Remove UTF-8 BOM
		if bytes.HasPrefix([]byte(keys[0]), bom) {
			keys[0] = string(bytes.Replace([]byte(keys[0]), bom, []byte(""), 1))
		}
		for key := range validator.Conf.Validation {
			key = stringutils.Trim(key)
			key = strings.TrimPrefix(key, " ")
			key = strings.TrimSuffix(key, " ")
			if !stringutils.CheckPresence(keys, key) {
				for _, header := range keys {
					for _, c := range header {
						fmt.Printf("%d-", c)
					}
					fmt.Printf("\n[%s]\n", header)
				}
				panic(fmt.Sprintf("headers %s does not contains the following header: [%s]", keys, key))
			}
		}
		var lines []string
		var row int
		for scanner.Scan() {
			lines = strings.Split(scanner.Text(), validator.Conf.Separator)
			if len(lines) != len(keys) {
				panic(fmt.Sprintf("Error on line %d", row))
			}
			// Iterating headers of the csv
			for i, key := range keys {
				field := lines[i]
				// Ignore the data that can be null
				if strings.HasSuffix(validator.Conf.Validation[key], "|NULLABLE") {
					continue
				}
				switch validator.Conf.Validation[key] {
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
				}
			}
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

//fmt.Println(output.String())
