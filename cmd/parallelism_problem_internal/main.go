package main

import (
	"errors"
	"sync"
	"time"
)

// Есть источник данных, это может (быть база данных, АПИ м пр.) генерирующий последовательность данных
// c определенной частотой "Ч" операций/секунду,
// в данном примере, это функция producer, выдающая последовательно надор натуральных чисел от 1 до limit.
// Есть приемник данных, проводящий сложные манипуляции с входящими данными и к примеру сохраняющий результат в другую базу данных,
// умеющий обрабатывать входящие данные с частотой "Ч"/N операциций в секунду, в данном примере это функция processor, вычисляющая квадраты входящего значения,
// где с помощью паузы выполнения эмулируется длительное выполнений операции.
// Для эффективного выполнения задачи, требуется согласовать источник данных и примник данных, путем параллельной обработки в потребителе,
// с ограничение степени параллелизма обработки в размере concurrencySize.
// Таким образом потребитель при получении данных запускает не более чем concurrencySize обработчиков,
//
//                               processor
//		producer -> consumer ->  processor -> terminator (выводит на экран результат, в наеш случае, суммы квадратов входящих наруальных чисел)
//                               ...
//                               processor
//
// При возникновении ошибки обработки, требуется отменить все последующие расчеты, и вернуть ошибку

const (
	limit           = 1000
	concurrencySize = 5
)

func producer(limit int) chan int {
	tasks := make(chan int)

	go func() {
		defer func() {
			recover()
		}()

		for i := 1; i <= limit; i++ {
			tasks <- i
			time.Sleep(time.Duration(5000000000/limit) * time.Nanosecond)
		}

		close(tasks)
	}()

	return tasks
}

func processor(i int) (int, error) {
	if i == 10 {
		return 0, errors.New("i hate 5")
	}
	time.Sleep(5 * time.Second)
	return i * i, nil
}

func terminator(results chan int) {
	go func() {
		for value := range results {
			println(value)
		}
	}()
}

func consumer(tasks <-chan int, results chan<- int, concurrencySize int) error {
	errCh := make(chan error)
	var wg sync.WaitGroup

	for j := 0; j < concurrencySize; j++ {
		wg.Add(1)

		go func() {
			defer func() {
				recover()
				wg.Done()
			}()

			for value := range tasks {
				result, err := processor(value)
				if err != nil {
					errCh <- err
					return
				}
				results <- result
			}
		}()
	}

	var err error
	go func() {
		err = <-errCh
		close(errCh)
		close(results)
	}()

	wg.Wait()
	if err == nil {
		errCh <- nil
	}

	return err
}

func main() {
	tasks := producer(limit)

	results := make(chan int)
	terminator(results)
	if err := consumer(tasks, results, concurrencySize); err != nil {
		close(tasks)
		println(err.Error())
	}
}
