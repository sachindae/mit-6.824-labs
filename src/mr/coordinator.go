package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

// Time to wait before rerunning a task
const TaskTimeoutTime int64 = 10 

type MapTask struct {
	FileName string
	TimeRunning int64
}

type ReduceTask struct {
	PartitionNum int
	TimeRunning int64
}

type Coordinator struct {
	// Your definitions here.

	// State of Mapping/Reducing 
	MapState State 
	ReduceState State

	// Map task tracking
	NewMaps []MapTask
	RunningMaps []MapTask
	CompletedMaps []MapTask

	// Reduce task tracking
	NewReduces []ReduceTask
	RunningReduces []ReduceTask
	CompletedReduces []ReduceTask

	nReduce int
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// RPC handler that assigns a task to worker if possible.
//
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce

	// Assign Map task if possible
	if c.MapState == NOTSTARTED || c.MapState == RUNNING {

		reply.DoMap = true
		c.MapState = RUNNING

		if len(c.NewMaps) > 0 {
			// Check if any unassigned files
			mapTask := c.NewMaps[0]
			mapTask.TimeRunning = time.Now().Unix()
			reply.Fname = mapTask.FileName
			c.NewMaps = c.NewMaps[1:]
			c.RunningMaps = append(c.RunningMaps, mapTask)
			
		} else {
			// Check if any assigned files running > 10 seconds
			for index, mapTask := range c.RunningMaps {
				if time.Now().Unix() - mapTask.TimeRunning > TaskTimeoutTime {
					reply.Fname = mapTask.FileName
					c.RunningMaps[index].TimeRunning = time.Now().Unix()
					return nil
				}
			}

			// No available map tasks for worker
			reply.Busy = true
		}
		

	// Assign Reduce task if possible
	} else if c.ReduceState == NOTSTARTED || c.ReduceState == RUNNING {

		reply.DoReduce = true
		c.ReduceState = RUNNING

		if len(c.NewReduces) > 0 {
			// Check if any unassigned reduce tasks
			newReduce := c.NewReduces[0]
			newReduce.TimeRunning = time.Now().Unix()
			reply.PartitionNum = newReduce.PartitionNum
			c.NewReduces = c.NewReduces[1:]
			c.RunningReduces = append(c.RunningReduces, newReduce)
			
		} else {
			// Check if any assigned reduce tasks running > 10 seconds
			for index, reduceTask := range c.RunningReduces {
				if time.Now().Unix() - reduceTask.TimeRunning > TaskTimeoutTime {
					reply.PartitionNum = reduceTask.PartitionNum
					c.RunningReduces[index].TimeRunning = time.Now().Unix()
					return nil
				}
			}

			// No available map tasks for worker
			reply.Busy = true
		}
	} 

	return nil
}

//
// RPC handler that marks a task as completed
//
func (c *Coordinator) CompleteTask(args *TaskArgs, reply *TaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	
	if args.MapFin {
		// Mark Map task as finished
		for index, mapTask := range c.RunningMaps {
			if mapTask.FileName == args.Fname {
				c.RunningMaps = append(c.RunningMaps[:index], c.RunningMaps[index+1:]...)
				c.CompletedMaps = append(c.CompletedMaps, mapTask)
				break
			}
		}

		// Check if any remaining Map tasks
		if len(c.NewMaps) == 0 && len(c.RunningMaps) == 0 {
			c.MapState = FINISHED
		}

	} else if args.ReduceFin {
		// Mark Reduce task as completed
		for index, reduceTask := range c.RunningReduces {
			if reduceTask.PartitionNum == args.PartitionNum {
				c.RunningReduces = append(c.RunningReduces[:index], c.RunningReduces[index+1:]...)
				c.CompletedReduces = append(c.CompletedReduces, reduceTask)
				break
			}
		}

		// Check if any remaining Reduce tasks
		if len(c.NewReduces) == 0 && len(c.RunningReduces) == 0 {
			c.ReduceState = FINISHED
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
	ret := false

	// Your code here.

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if Map and Reduce tasks are finished
	if c.MapState == FINISHED && c.ReduceState == FINISHED {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// log.Printf("%d files, %d reduce tasks!\n", len(files), nReduce)
	
	// Initialize Map tasks
	for _, file := range files {
		mapTask := MapTask{file, 0}
		c.NewMaps = append(c.NewMaps, mapTask)
	}

	// Initialize Reduce tasks
	for i := 0; i < nReduce; i++ {
		reduceTask := ReduceTask{i, 0}
		c.NewReduces = append(c.NewReduces, reduceTask)
	}

	c.nReduce = nReduce
	
	//log.Printf("Coordinator %v\n", c)

	

	c.server()
	return &c
}
