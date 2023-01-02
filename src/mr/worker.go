package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "time"
import "encoding/json"
import "path/filepath"
import "sort"

// for sorting by key.
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// Loop till no more tasks (all finished)
	for {

		// RPC request to coordinator to get task
		rpcReply := CallGetTask()
		
		// Check if no tasks available currently and sleep if so
		if rpcReply.Busy {
			time.Sleep(time.Second)
			continue
		}

		// Check if jobs all done, terminate worker if so
		if !rpcReply.DoMap && !rpcReply.DoReduce {
			return
		}

		// Do either Map or Reduce task depending on which assigned
		if rpcReply.DoMap { 
			// Map task assigned

			// Read input file for Map task
			filename := rpcReply.Fname
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
				continue
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
				continue
			}
			file.Close()

			// Run mapper to compute intermediate keyValue pairs
			intermediate := mapf(filename, string(content))
	
			// Create intermediate partition files
			inames := []string{}
			ifiles := [](*json.Encoder){}
			for i := 0; i < rpcReply.NReduce; i++ {
				iname := fmt.Sprintf("mr-int-%v-%s", i, filepath.Base(filename))
				inames = append(inames, iname)
				ifile, _ := os.Create(iname)
				ifiles = append(ifiles, json.NewEncoder(ifile))
			}
			
			// Write keyValue pairs to appropriate intermediate partitions
			for _, keyValuePair := range intermediate {
				partitionNum := ihash(keyValuePair.Key) % rpcReply.NReduce
				ifiles[partitionNum].Encode(&keyValuePair)
			}
			
			// Notify coordinator of completion of Map task
			CallCompleteTask(filename, -1, true, false)

		} else if rpcReply.DoReduce {
			// Reduce task assigned

			// Get list of intermediate files to read according to partition number
			partitionNum := rpcReply.PartitionNum
			intermediateFiles, err := filepath.Glob(fmt.Sprintf("mr-int-%d-%v", partitionNum, "*"))
			if err != nil{
				log.Println("Glob error")
				continue
			}

			// Read intermediate files and store key value pairs
			kva := []KeyValue{}
			for _, fname := range intermediateFiles {
				file, err := os.Open(fname)
				if err != nil {
					log.Fatalf("cannot open %v", fname)
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}

			}

			// Sort key value pairs
			sort.Sort(ByKey(kva))

			// Create partition output file and call reduce on each distinct key
			oname := fmt.Sprintf("mr-out-%d", partitionNum)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()
			
			// Notify coordinator of completion of Reduce task
			CallCompleteTask("", partitionNum, false, true)
		}

	}
}

//
// RPC function to get next task for worker if any available
//
func CallGetTask() (reply TaskReply) {

	// declare an argument structure.
	args := TaskArgs{}

	// declare a reply structure.
	reply = TaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}

	return
}

//
// RPC function to get next task for worker if any available
//
func CallCompleteTask(Fname string, PartitionNum int, MapFin, ReduceFin bool) {

	// declare an argument structure.
	args := TaskArgs{}

	// fill in the argument(s).
	args.Fname = Fname
	args.PartitionNum = PartitionNum
	args.MapFin = MapFin
	args.ReduceFin = ReduceFin

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
