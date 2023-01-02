package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type State int

const (
	NOTSTARTED State = 0
	RUNNING State = 1
	FINISHED State = 2
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskArgs struct {
	MapFin bool
	ReduceFin bool
	Fname string
	PartitionNum int
}

type TaskReply struct {
	DoMap bool
	DoReduce bool
	Busy bool
	Fname string
	PartitionNum int
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
