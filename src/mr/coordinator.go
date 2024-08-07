package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	// "fmt"

)

type TaskType int
const (
	MapTask = iota
	WaitingTask
	ReduceTask
	OverTask
)

type Task struct {
	Id int
	File string
	Typ TaskType
	NMap int
	NReduce int
}

type Coordinator struct {
	// Your definitions here.
	files []string
	nReduce int
	mapAllocatedNum int
	mapCompletedNum int
	reduceAllocatedNum int
	reduceCompletedNum int
	isOver bool
	mu sync.Mutex
	mapOK map[int]bool
	mapLastAllocatedTime map[int]time.Time
	reduceOK map[int]bool
	reduceLastAllocatedTime map[int]time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.	
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetMapTask(taskId int) Task {
	return Task{
		Id: taskId,
		File: c.files[taskId],
		Typ: MapTask,
		NReduce: c.nReduce,
		NMap: len(c.files)}
}

func (c *Coordinator) GetReduceTask(taskId int) Task {
	return Task{
		Id: taskId,
		Typ: ReduceTask,
		NReduce: c.nReduce,
		NMap: len(c.files)}
}

func (c *Coordinator) GetTask(args *EmptyArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapCompletedNum < len(c.files) {
		nowTime := time.Now()
		for i := 0; i < len(c.files); i++ {
			if !c.mapOK[i] && nowTime.Sub(c.mapLastAllocatedTime[i]) > time.Second * 10 {
				reply.Task = c.GetMapTask(i)
				c.mapLastAllocatedTime[i] = nowTime
				c.mapAllocatedNum++
				return nil
			}
		}
		reply.Task = Task{Typ: WaitingTask}
	} else if c.reduceCompletedNum < c.nReduce {
		nowTime := time.Now()
		for i := 0; i < c.nReduce; i++ {
			if !c.reduceOK[i] && nowTime.Sub(c.reduceLastAllocatedTime[i]) > time.Second * 10 {
				reply.Task = c.GetReduceTask(i)
				c.reduceLastAllocatedTime[i] = nowTime
				c.reduceAllocatedNum++
				return nil
			}
		}
		reply.Task = Task{Typ: WaitingTask}
	} else {
		reply.Task = Task{Typ: OverTask}
	}
	return nil
}

func (c *Coordinator) DoneMap(args *IdArgs, reply *EmptyArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.mapOK[args.Id] {
		c.mapCompletedNum++
		c.mapOK[args.Id] = true
		// fmt.Printf("id: %d num: %d\n",args.Id,c.mapCompletedNum)
	}
	return nil
}

func (c *Coordinator) DoneReduce(args *IdArgs, reply *EmptyArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.reduceOK[args.Id] {
		c.reduceCompletedNum++
		c.reduceOK[args.Id] = true
		if c.reduceCompletedNum == c.nReduce {
			go func(c *Coordinator) {
				time.Sleep(time.Second * 2)
				c.mu.Lock()
				c.isOver = true
				c.mu.Unlock()
			}(c)
		}
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	ret := c.isOver
	c.mu.Unlock()
	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: files,
		nReduce: nReduce,
		mapAllocatedNum: 0,
		mapCompletedNum: 0,
		reduceAllocatedNum: 0,
		reduceCompletedNum: 0,
		isOver: false,
		mapOK: make(map[int]bool),
		mapLastAllocatedTime: make(map[int]time.Time),
		reduceOK: make(map[int]bool),
		reduceLastAllocatedTime: make(map[int]time.Time)}

	// Your code here.


	c.server()
	return &c
}
