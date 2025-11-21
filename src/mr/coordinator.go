package mr

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const timeout = time.Second * 10

type Operation int

const (
	Map Operation = iota
	Reduce
	Wait
	Exit
)

type JobStage int

const (
	JobStageMap JobStage = iota
	JobStageReduce
)

type Task struct {
	ID         int64
	NReduce    int
	Type       Operation
	Partition  int
	MapTaskIDs []int64
	File       string
	StartTime  time.Time
}

type Coordinator struct {
	mu *sync.Mutex

	nReduce       int
	JobStage      JobStage
	MapTasks      *list.List
	ReduceTasks   *list.List
	taskIDCounter *atomic.Int64
	activeTasks   map[int64]Task

	mapTaskIDs []int64
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	newTask := &Task{Type: Wait, NReduce: c.nReduce, StartTime: time.Now(), ID: c.taskIDCounter.Add(1)}
	reply.Task = newTask
	if c.Done() {
		reply.Task.Type = Exit
		return nil
	}

	for _, task := range c.activeTasks {
		if task.StartTime.Add(timeout).Before(time.Now()) {
			fmt.Println(fmt.Sprintf("task %d timed out, spawning new task: %d", task.ID, newTask.ID))
			delete(c.activeTasks, task.ID)
			newTask.Type = task.Type
			newTask.File = task.File
			newTask.MapTaskIDs = task.MapTaskIDs
			newTask.Partition = task.Partition
			c.activeTasks[newTask.ID] = *newTask
			return nil
		}
	}

	switch c.JobStage {
	case JobStageMap:
		newFile := c.MapTasks.Front()
		if newFile == nil {
			return nil
		}
		c.MapTasks.Remove(newFile)
		newTask.Type = Map
		newTask.File = newFile.Value.(string)
	case JobStageReduce:
		partition := c.ReduceTasks.Front()
		if partition == nil {
			return nil
		}
		c.ReduceTasks.Remove(partition)
		newTask.Type = Reduce
		newTask.Partition = partition.Value.(int)
		newTask.MapTaskIDs = c.mapTaskIDs
	default:
		return errors.New("invalid job stage")
	}
	c.activeTasks[newTask.ID] = *newTask
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.activeTasks[args.Task.ID]
	if !ok {
		return nil
	}
	delete(c.activeTasks, args.Task.ID)
	switch args.Task.Type {
	case Map:
		c.mapTaskIDs = append(c.mapTaskIDs, args.Task.ID)
	case Reduce:
	case Wait:
	case Exit:
	}

	if len(c.activeTasks) == 0 && c.MapTasks.Len() == 0 {
		c.JobStage = JobStageReduce
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.MapTasks.Len() == 0 && c.ReduceTasks.Len() == 0 && len(c.activeTasks) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := list.New()
	for _, file := range files {
		mapTasks.PushBack(file)
	}
	reduceTasks := list.New()
	for i := range nReduce {
		reduceTasks.PushBack(i)
	}
	c := Coordinator{
		mu:            &sync.Mutex{},
		nReduce:       nReduce,
		JobStage:      JobStageMap,
		MapTasks:      mapTasks,
		ReduceTasks:   reduceTasks,
		taskIDCounter: &atomic.Int64{},
		activeTasks:   make(map[int64]Task),
	}
	c.server()
	return &c
}
