package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		taskReply := &GetTaskReply{}
		got := CallGetTask(&GetTaskArgs{}, taskReply)
		if !got || taskReply.Task == nil {
			panic("GetTask failed")
		}

		task := taskReply.Task
		switch task.Type {
		case Map:
			// read file
			contents, err := os.ReadFile(task.File)
			if err != nil {
				panic(err)
			}
			// pass file through mapper
			keyValues := mapf(task.File, string(contents))
			if len(keyValues) == 0 {
				break
			}
			// partition into buckets
			buckets := make([][]KeyValue, task.NReduce)
			for _, kv := range keyValues {
				idx := ihash(kv.Key) % task.NReduce
				buckets[idx] = append(buckets[idx], kv)
			}

			for i, bucket := range buckets {
				tempFileName := fmt.Sprintf("mr-tmp-%d", task.ID)
				tempFile, _ := os.CreateTemp("", tempFileName)

				enc := json.NewEncoder(tempFile)
				for _, kv := range bucket {
					_ = enc.Encode(&kv)
				}
				_ = tempFile.Close()
				fileName := fmt.Sprintf("mr-inter-%d-%d", i, task.ID)
				err = os.Rename(tempFile.Name(), fileName)
				if err != nil {
					panic(err)
				}
			}

			CallTaskDone(&TaskDoneArgs{Task: *task}, &TaskDoneReply{})
		case Reduce:
			// gather data
			partition := task.Partition
			mapTaskIDs := task.MapTaskIDs
			intermediateFileNames := make([]string, 0, len(mapTaskIDs))
			for _, taskID := range mapTaskIDs {
				intermediateFileNames = append(intermediateFileNames, fmt.Sprintf("mr-inter-%d-%d", partition, taskID))
			}

			// read data
			var keyVals []KeyValue
			for _, fileName := range intermediateFileNames {
				file, err := os.Open(fileName)
				if err != nil {
					panic(err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err == io.EOF {
						break
					}
					keyVals = append(keyVals, kv)
				}
				_ = file.Close()
			}

			// sort
			sort.Slice(keyVals, func(i, j int) bool {
				return keyVals[i].Key < keyVals[j].Key
			})

			outFileName := fmt.Sprintf("mr-tmp-out-%d", task.Partition)
			outFile, _ := os.CreateTemp("", outFileName)
			for i := 0; i < len(keyVals); {
				j := i + 1
				for j < len(keyVals) && keyVals[j].Key == keyVals[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, keyVals[k].Value)
				}
				output := reducef(keyVals[i].Key, values)
				_, _ = outFile.Write([]byte(fmt.Sprintf("%s %s\n", keyVals[i].Key, output)))
				i = j
			}
			_ = outFile.Close()
			_ = os.Rename(outFile.Name(), fmt.Sprintf("mr-out-%d", task.Partition))
			CallTaskDone(&TaskDoneArgs{Task: *task}, &TaskDoneReply{})
		case Wait:
			time.Sleep(1 * time.Second)
		case Exit:
			return
		default:
			panic("unhandled default case")
		}
	}
}

func CallGetTask(args *GetTaskArgs, reply *GetTaskReply) bool {
	return call("Coordinator.GetTask", args, reply)
}

func CallTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) bool {
	return call("Coordinator.TaskDone", args, reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
