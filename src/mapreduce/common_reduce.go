package mapreduce

import (
	"os"
	"encoding/json"
	"io"
	"log"
	"bytes"
	"io/ioutil"
)

type ByKey []KeyValue
func (a ByKey) Len() int {return len(a)}
func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i]}
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	w, err := os.OpenFile(outFile, os.O_APPEND|os.O_WRONLY, 0600)

	if err != io.EOF && err!= nil {
		w, _ = os.Create(outFile)
	}

	for mapNum := 0; mapNum < nMap; mapNum++ {
		reduceFileName := reduceName(jobName, mapNum, reduceTaskNumber)
		f, err := ioutil.ReadFile(reduceFileName)
		errCheck(err)

		read := bytes.NewBuffer(f)
		write := new(bytes.Buffer)
		dec := json.NewDecoder(read)
		enc := json.NewEncoder(write)

		var keyValuePairs []KeyValue
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			keyValuePairs = append(keyValuePairs, kv)
		}
		keys := make(map[string]bool)
		for _, key := range keyValuePairs {
			keys[key.Key] = true
		}

		for key := range keys {
			var values []string
			for _, keyValue := range keyValuePairs {
				if keyValue.Key == key {
					values = append(values, keyValue.Value)
				}
			}
			enc.Encode(KeyValue{key, reduceF(key, values)})
			w.WriteString(write.String())
		}
	}
	w.Close()


	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
}
