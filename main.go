package main

import (
	"flag"
	csvutils "github.com/alessiosavi/GoGPUtils/csv"
	fileutils "github.com/alessiosavi/GoGPUtils/files"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"github.com/alessiosavi/GoWarehouseValidator/datastructure"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"time"
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
			rawData, err := ioutil.ReadAll(file)
			if err != nil {
				panic(err)
			}
			headers, data, err := csvutils.ReadCSV(rawData, stringutils.GetFirstRune(toValidate.Separator))

			for _, conf := range validator.Conf.Conf {
				validateData, err := validator.ValidateData(headers, data, conf)
				if err != nil {
					panic(err)
				}

				if len(validateData) > 0 {
					var errorCSV [][]string = append([][]string{}, []string{stringutils.JoinSeparator(",", headers...)})
					for _, err := range validateData {
						errorCSV = append(errorCSV, []string{err.Row})
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
