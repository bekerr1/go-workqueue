package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

const desiredWorkerCount int = 5
const unknownJobId int = 0

var realWorkerCount int

var wg sync.WaitGroup
var logr *log.Logger

type StringWorker struct {
	ctx      context.Context
	s1       string
	s2       string
	combined string
}

func (s *StringWorker) PerformWork() {
	s.combined = s.s1 + s.s2
	logr.Println()
	logr.Println("Finished performing string combine", s.combined)
	logr.Println("...sleeping for a bit")
	time.Sleep(10 * time.Second)
}

func (s *StringWorker) JobId() string {
	return s.ctx.Value("jobid").(string)
}

type Workable interface {
	PerformWork()
	JobId() string
	//WorkContext() context.Context
}

type WorkQueue struct {
	mux         sync.Mutex
	waitingWork []chan bool
	// map of work id -> channel
	busyWork        map[string]chan bool
	work            []Workable
	workReady       chan bool
	workWaiting     chan bool
	workStop        chan bool
	waitLoopRunning bool
}

func NewWorkQueue() *WorkQueue {

	var waiting []chan bool
	busy := make(map[string]chan bool)
	workReady := make(chan bool, desiredWorkerCount)
	workWaiting := make(chan bool, desiredWorkerCount)
	workStop := make(chan bool, desiredWorkerCount)
	var wq []Workable
	return &WorkQueue{
		waitingWork:     waiting,
		busyWork:        busy,
		work:            wq,
		workReady:       workReady,
		workWaiting:     workWaiting,
		workStop:        workStop,
		waitLoopRunning: false,
	}
}

func signalNextForWork(wq *WorkQueue) {
	wq.mux.Lock()
	if len(wq.waitingWork) > 0 {
		logr.Println()
		logr.Println("Worker thread available")
		threadChan := wq.waitingWork[0]
		logr.Println("Sending signal on channel ", &threadChan)
		wq.waitingWork = wq.waitingWork[1:]
		threadChan <- true
	} else {
		logr.Println("no worker threads available..work will be dequeued when one isnt busy")
	}
	wq.mux.Unlock()
}

func (wq *WorkQueue) RunWaitLoop() {
	logr.Println()
	logr.Println("All workers are busy. Running wait loop to clear out work queue")
	wg.Add(1)

	go func() {
	Loop:
		for len(wq.work) > 0 {
			logr.Printf("len wq.work check claims > 0. Actual %v\n", len(wq.work))
			select {
			case _ = <-wq.workWaiting:
				logr.Println()
				logr.Printf("got work waiting signal from goroutine on channel %v\n", &wq.workWaiting)
				logr.Printf("len wq.work during signal %v\n", len(wq.work))
				if len(wq.work) == 0 {
					break Loop
				}
				signalNextForWork(wq)
			}
			logr.Printf("len wq.work after signal %v\n", len(wq.work))
		}
		logr.Printf("len wq.work check claims 0. Actual %v\n", len(wq.work))
		wg.Done()

		wq.mux.Lock()
		wq.waitLoopRunning = false
		wq.mux.Unlock()
	}()
}

func (wq *WorkQueue) RunWorkLoop() {
	wq.startWorkers()

	logr.Println()
	logr.Println("Starting main WQ run loop on goroutine")
	wg.Add(1)
	go func() {
		for {
			select {
			case _ = <-wq.workReady:
				logr.Println("Work ready signal...check if there is a worker thread waiting")
				signalNextForWork(wq)
			case _ = <-wq.workStop:
				logr.Println("Stop main work loop")
				wg.Done()
				break
			}
		}
		logr.Println("exiting RunWorkLoop func")
	}()
}

func (wq *WorkQueue) EnqueueWork(w Workable) error {

	wq.mux.Lock()
	wq.work = append(wq.work, w)
	if len(wq.waitingWork) > 0 {
		wq.workReady <- true
	} else {
		logr.Println()
		logr.Println("no waiting work. Starting waiting loop")
		if !wq.waitLoopRunning {
			wq.RunWaitLoop()
			wq.waitLoopRunning = true
		}
	}
	wq.mux.Unlock()
	return nil
}

func (wq *WorkQueue) startWorkers() {

	for x := 0; x < desiredWorkerCount; x++ {
		msgch := make(chan bool)
		quitch := make(chan bool)
		go func(workIndex int, c, q chan bool) {

			wq.mux.Lock()
			wq.waitingWork = append(wq.waitingWork, c)
			realWorkerCount += 1
			logr.Printf("Inserted channel %v with address %v into waitingQueue\n", realWorkerCount, &c)
			logr.Printf("Starting worker %v\n", realWorkerCount)
			wq.mux.Unlock()

			logr.Println("waiting for work on work channel")
			var currentWork Workable
		Loop:
			for {
				// assume my channel has been removed from waiting queue
				select {
				case _ = <-c:
					logr.Println()
					logr.Println("Got message on a waiting work channel")
					wq.mux.Lock()
					currentWork = wq.work[0]
					logr.Println("currentwork job id: ", currentWork.JobId())
					wq.busyWork[currentWork.JobId()] = c
					wq.work = wq.work[1:]
					logr.Println("waiting channels ", wq.waitingWork, len(wq.waitingWork))
					logr.Println("working channels ", wq.busyWork, len(wq.busyWork))
					wq.mux.Unlock()
					break
				case _ = <-q:
					wg.Done()
					break Loop
				}

				logr.Println()
				currentWork.PerformWork()

				wq.mux.Lock()
				logr.Printf("pushing channel with address %v back in waiting work", &c)
				delete(wq.busyWork, currentWork.JobId())
				wq.waitingWork = append(wq.waitingWork, c)
				wq.workWaiting <- true
				logr.Println("waiting channels ", wq.waitingWork, len(wq.waitingWork))
				logr.Println("working channels ", wq.busyWork, len(wq.busyWork))
				wq.mux.Unlock()
			}
		}(x, msgch, quitch)
	}
}

func main() {
	fmt.Println("vim-go")

	f, err := os.OpenFile("output.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	logr = log.New(f, "", log.LstdFlags)
	logr.Println("=======================================")
	logr.Println("\t\tLogr initialized")
	logr.Println("=======================================")

	wg.Add(desiredWorkerCount)
	wq := NewWorkQueue()
	//wq.startWorkers()
	wq.RunWorkLoop()

	logr.Println("Starting workers")
	logr.Println("waiting for workers to finish")

	logr.Println("Sleeping to wait for workers")
	time.Sleep(5 * time.Second)
	logr.Println()
	logr.Println("About to work")
	var sctx context.Context
	sctx = context.WithValue(sctx, "jobid", "0000a0")
	swork := &StringWorker{
		ctx:      sctx,
		s1:       "Hello, ",
		s2:       "World!",
		combined: "",
	}
	var sctx1 context.Context
	sctx1 = context.WithValue(sctx1, "jobid", "0000a1")
	swork1 := &StringWorker{
		ctx:      sctx1,
		s1:       "Foo, ",
		s2:       "Bar!",
		combined: "",
	}
	var sctx2 context.Context
	sctx2 = context.WithValue(sctx2, "jobid", "0000a2")
	swork2 := &StringWorker{
		ctx:      sctx2,
		s1:       "Bar, ",
		s2:       "Baz!",
		combined: "",
	}
	var sctx3 context.Context
	sctx3 = context.WithValue(sctx3, "jobid", "0000a3")
	swork3 := &StringWorker{
		ctx:      sctx3,
		s1:       "Baz, ",
		s2:       "Spaz!",
		combined: "",
	}
	var sctx4 context.Context
	sctx4 = context.WithValue(sctx4, "jobid", "0000a4")
	swork4 := &StringWorker{
		ctx:      sctx4,
		s1:       "Raz, ",
		s2:       "Taz!",
		combined: "",
	}
	var sctx5 context.Context
	sctx5 = context.WithValue(sctx5, "jobid", "0000a5")
	swork5 := &StringWorker{
		ctx:      sctx5,
		s1:       "Mad, ",
		s2:       "Lad!",
		combined: "",
	}
	var sctx6 context.Context
	sctx6 = context.WithValue(sctx5, "jobid", "0000a6")
	swork6 := &StringWorker{
		ctx:      sctx6,
		s1:       "Dad, ",
		s2:       "Sad!",
		combined: "",
	}

	_ = wq.EnqueueWork(swork)
	time.Sleep(1 * time.Second)
	_ = wq.EnqueueWork(swork1)
	time.Sleep(1 * time.Second)
	_ = wq.EnqueueWork(swork2)
	time.Sleep(1 * time.Second)
	_ = wq.EnqueueWork(swork3)
	time.Sleep(1 * time.Second)
	_ = wq.EnqueueWork(swork4)
	time.Sleep(1 * time.Second)
	_ = wq.EnqueueWork(swork5)
	time.Sleep(1 * time.Second)
	_ = wq.EnqueueWork(swork6)
	time.Sleep(1 * time.Second)

	for {
	}
	wg.Wait()
}
