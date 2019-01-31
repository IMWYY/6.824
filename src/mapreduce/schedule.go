package mapreduce

import (
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	wg := sync.WaitGroup{}
	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		go func(index int) {
			for {
				worker := <-mr.registerChannel
				arg := &DoTaskArgs{
					JobName:       mr.jobName,
					File:          mr.files[index],
					Phase:         phase,
					TaskNumber:    index,
					NumOtherPhase: nios,
				}

				ok := call(worker, "Worker.DoTask", arg, new(struct{}))
				if ok {
					wg.Done()
					mr.registerChannel <- worker
					break
				}
			}
		}(i)
	}

	wg.Wait()
	debug("Schedule: %v phase done\n", phase)
}
