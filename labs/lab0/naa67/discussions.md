1) Moving data into an unbuffered channel hangs until there is a concurrent receiver ready to empty it. A buffered channel has a specified size k where k units of data may be moved into it and wait for a receiver to empty them one by one. Once the buffer is full, additional attempts to move data into the channel will hang.

2) The default in Go is unbuffered.

3) There is a deadlock because the call to `ch <- "hello world!"` hangs until a receiver can get the data, but there is no concurrent receiver.

4) `<-chan T` indicates that the function may only read data from the channel, `chan<- T` indicates that the function may only write data to the channel, and `chan T` is a simple bidirectional channel that doesn't enforce those compile-time semantics. It is also important to note that closing a channel is the sender's responsibility, so the function may only close a `chan T` or a `chan<- T`.

5) If it is a nonempty buffered channel, it will receive the values one-by-one per read until it is empty. If it is empty, or if it is unbuffered, reads will return the type's zero value with a corresponding `false` value to distinguish between actual zeros in the channel. Reading a `nil` channel blocks the corresponding goroutine forever.

6) If `ch` is non-nil, the loop will terminate when the channel is closed and drained. If `ch` is nil, the goroutine running the function will block forever.

7) `context.Context.Done()` returns a channel which is closed when the context is done. We can wait for this by reading the channel, since the reading goroutine will hang until the channel is closed. If blocking is not desired, a typical pattern to check if it's done is
```go
// ...
select {
case <-ctx.Done():
    // done, execute some code to move on
    // ctx.Err() contains the reason the context was canceled
default:
    // not done, execute some code to move on
}
```

8) Assuming the program lasts longer than 3 seconds past this loop, it will print the following:
```
all done!
1
2
3
```
If the program ends right after this snippet, then it will only print
```
all done!
```
since the main goroutine will exit.

9) To prevent the program from exiting before the goroutines finish their work, we can use the `sync.WaitGroup` primitive to wait for them to finish. We can call `wg.Add(1)` before each `go func() ...` call, call `defer wg.Done()` at the start of the `func()` body, then call `wg.Wait()` right after the loop, if the desire is to have them print before the "all done!" statement. This looks like:
```go
var wg sync.WaitGroup
for i := 1; i <= 3; i++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        time.Sleep(time.Duration(i) * time.Second)
        fmt.Printf("%d\n", i)
    }()
}
wg.Wait()
fmt.Println("all done!")
```

10) A mutex semantically defines a critical section of code which only one goroutine may execute at a time, and the core idea of its use is that the aforementioned goroutine "owns" the lock and will unlock it once it is finished. While a correctly configured semaphore (maximum weight 1, behaves like a binary semaphore) can achieve the same goal, semaphores are semantically used to gate access to a shared resource and doesn't have the same ownership ideology. Also, a weighted semaphore allows a number of goroutines to access shares of a particular resource at the same time, whereas a mutex is always limited to one at a time.

11)
```
[]
0
true

0
<nil>
{}
```

12) It is an empty struct of size 0. Using it in a channel is useful when the channel only acts as a signal for something and doesn't actually need to transfer any data.

13) In new versions of Go, the loop declares a new `i` for each iteration, so the `i` captured by the goroutines in the previous iterations of the loop are not modified. In past versions of Go, by the time the goroutines woke up, the loop would've exited with `i=4` and so they would all print `4` since the same `i` is captured by reference by each goroutine and is modified in the loop.
