package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	for {
		task := GetTask()
		// fmt.Printf("type: %v\n",task.Typ)
		switch task.Typ {
		case MapTask:
			DoMap(task, mapf)
			DoneMap(task.Id)
		case WaitingTask:
			time.Sleep(time.Second)
		case ReduceTask:
			DoReduce(task, reducef)
			DoneReduce(task.Id)
		case OverTask:
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoMap(task Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.File)
	if err != nil {
		log.Fatalf("worker cannot open %v", file)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("worker cannot read %v", file)
	}
	intermediate := mapf(task.File, string(content))
	trans := make([][]KeyValue, task.NReduce)
	cnt:=0
	for _, kv := range intermediate {
		num := ihash(kv.Key) % task.NReduce
		trans[num] = append(trans[num], kv)
		if kv.Key == "a" {
			cnt++
		}
	}
	// fmt.Printf("%d: %d\n",task.Id, cnt)
	for i := 0; i < task.NReduce; i++ {
		outFileName := "mr-" + strconv.Itoa(task.Id) + "-" + strconv.Itoa(i)
		outFile, err := os.Open(outFileName)
		if err != nil {
			outFile, err = os.Create(outFileName)
			if err != nil {
				log.Fatalf("open or create temp file %s failed", outFileName)
			}
		}
		writer := bufio.NewWriter(outFile)
		for _, kv := range trans[i] {
			jsonBytes, _ := json.Marshal(kv)
			jsonBytes = append(jsonBytes, '\n')
			writer.Write(jsonBytes)
		}
		writer.Flush()
		outFile.Close()
	}
}

func DoReduce(task Task, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		inFileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.Id)
		inFile, err := os.Open(inFileName)
		if err != nil {
			log.Fatalf("open temp file %s failed", inFileName)
		}
		scanner := bufio.NewScanner(inFile)
		cnt:=0
		for scanner.Scan() {
			jsonBytes := scanner.Bytes()
			var kv KeyValue
			json.Unmarshal(jsonBytes, &kv)
			if len(kv.Key) > 0 {
				intermediate = append(intermediate, kv)
				if kv.Key == "a" {
					cnt++
				}
			}
		}
		// fmt.Printf("%d: %v\n",i, cnt)
		inFile.Close()
		os.Remove(inFileName)
	}
	outFileName := "mr-out-" + strconv.Itoa(task.Id)
	outFile, err := os.Create(outFileName)
	if err != nil {
		log.Fatalf("create %s failed", outFileName)
	}
	defer outFile.Close()
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

func DoneMap(Id int) {
	args := IdArgs{Id: Id}
	reply := EmptyArgs{}
	ok := call("Coordinator.DoneMap", &args, &reply)
	if !ok {
		fmt.Printf("send donemap message failed\n")
	}
}

func DoneReduce(Id int) {
	args := IdArgs{Id: Id}
	reply := EmptyArgs{}
	ok := call("Coordinator.DoneReduce", &args, &reply)
	if !ok {
		fmt.Printf("send donereduce message failed\n")
	}
}

func GetTask() Task {
	args := EmptyArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Printf("get task failed")
	}
	return reply.Task
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
