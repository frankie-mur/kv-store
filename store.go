package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

type LockableStore struct {
	m map[string]string
	sync.RWMutex
}

var store = LockableStore{
	m: make(map[string]string),
}

var logger TransactionLogger

var ErrNoSuchKey = errors.New("key doesn not exist")

func putHandler(w http.ResponseWriter, r *http.Request) {
	//Extract the body from the request
	key := mux.Vars(r)["key"]
	val, err := io.ReadAll(r.Body)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	//Store the value
	err = Put(key, string(val))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusCreated)

}

func getHandler(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]

	val, err := Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
	}
	w.Write([]byte(val))
}

func Put(key, value string) error {
	store.Lock()
	defer store.RWMutex.Unlock()

	store.m[key] = value
	logger.WritePut(key, value)

	return nil
}

func Get(key string) (string, error) {
	store.RLock()
	defer store.RUnlock()
	val, ok := store.m[key]
	if !ok {
		return "", ErrNoSuchKey
	}

	return val, nil
}

func Delete(key string) error {
	store.Lock()
	defer store.Unlock()

	delete(store.m, key)
	logger.WriteDelete(key)

	return nil
}

func initializeTransactionLog() error {
	var err error

	logger, err = NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()

	e := Event{}
	ok := true

	for ok && err == nil {
		select {
		case err, ok = <-errors: // Retrieve any errors
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete: // Got a DELETE event!
				err = Delete(e.Key)
			case EventPut: // Got a PUT event!
				err = Put(e.Key, e.Value)
			}
		}
	}

	logger.Run()

	return err
}
