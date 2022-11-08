package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorTaskStatus int

const (
	Idle CoordinatorTaskStatus = iota
	InProgress
	Completed
)

var mu sync.Mutex

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Coordinator struct {
	// Your definitions here.
	TaskQueue        chan *Task
	TaskMeta         map[int]*CoordinatorTask
	CoordinatorPhase State
	NReduce          int
	InPutFiles       []string
	Intermediates    [][]string
}

type Task struct {
	Input         string //源文件目录
	TaskState     State
	NReducer      int
	TaskNumber    int //task索引号
	Intermediates []string
	Output        string
}

type CoordinatorTask struct {
	TaskStatus    CoordinatorTaskStatus
	taskReference *Task
	StartTime     time.Time
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
	ret := false
	mu.Lock()
	defer mu.Unlock()
	if c.CoordinatorPhase == Exit {
		ret = true
	}
	// Your code here.

	return ret
}

func (c *Coordinator) createMapTask() {
	for idx, filename := range c.InPutFiles {
		taskMeta := Task{
			Input:      filename,
			TaskState:  Map,
			NReducer:   c.NReduce,
			TaskNumber: idx,
		}
		c.TaskQueue <- &taskMeta
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle,
			taskReference: &taskMeta,
		}
	}
}

func (c *Coordinator) createReduceTask() {
	for idx, files := range c.Intermediates {
		taskMeta := Task{
			TaskState:     Reduce,
			TaskNumber:    idx,
			NReducer:      c.NReduce,
			Intermediates: files,
		}
		c.TaskQueue <- &taskMeta
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle,
			taskReference: &taskMeta,
		}
	}
}

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue //这里取内容赋值，会避免指针指向的变化？
		c.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		c.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if c.CoordinatorPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		*reply = Task{TaskState: Wait}
	}
	return nil
}

func (c *Coordinator) TaskComplete(task *Task, replay *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	if task.TaskState != c.CoordinatorPhase || c.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		return nil
	}
	c.TaskMeta[task.TaskNumber].TaskStatus = Completed
	go c.processTaskResult(task)
	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		for reduceTaskId, filePath := range task.Intermediates {
			c.Intermediates[reduceTaskId] = append(c.Intermediates[reduceTaskId], filePath)
		}
		if c.allTaskDone() {
			c.createReduceTask()
			c.CoordinatorPhase = Reduce
		}
	case Reduce:
		if c.allTaskDone() {
			c.CoordinatorPhase = Exit
		}
	}
}

func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:        make(chan *Task, max(nReduce, len(files))),
		TaskMeta:         make(map[int]*CoordinatorTask),
		CoordinatorPhase: Map,
		NReduce:          nReduce,
		InPutFiles:       files,
		Intermediates:    make([][]string, nReduce),
	}

	c.createMapTask()

	// Your code here.

	c.server()

	go c.catchTimeOut()
	return &c
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.CoordinatorPhase == Exit {
			mu.Unlock()
			return
		}
		for _, coordinatorTask := range c.TaskMeta {
			if coordinatorTask.TaskStatus == InProgress && time.Now().Sub(coordinatorTask.StartTime) > 10*time.Second {
				c.TaskQueue <- coordinatorTask.taskReference
				coordinatorTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}
