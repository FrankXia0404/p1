package lsp

import "time"

func epoch(resetEpochCount chan bool, delay time.Duration, cb func(), epochCount chan int) {
	t := time.NewTimer(delay)
	count := 0
	for {
		select {
		case reset := <-resetEpochCount:
			if reset {
				count = 0
			} else {
				ltrace.Println("Epoch Return")
				return
			}
		case <-t.C:
			count++
			cb()
			t = time.NewTimer(delay)
			epochCount <- count
		}
	}
}

func epochServer(connId int, resetEpochCount chan bool, delay time.Duration, cb func(int), epochCount chan int) {
	t := time.NewTimer(delay)
	count := 0
	for {
		select {
		case reset := <-resetEpochCount:
			if reset {
				count = 0
			} else {
				return
			}
		case <-t.C:
			cb(connId)
			count++
			t = time.NewTimer(delay)
			epochCount <- count
		}
	}
}