package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/recoilme/pudge"
)

func main() {

	//cfg := &pudge.Config{
	//  SyncInterval: 1} // every second fsync
	/*
	   db, err := pudge.Open("db", cfg)
	   if err != nil {
	       //log.Panic(err)
	       print(err)
	       panic(err)
	   }
	*/

	//db_state, _ := pudge.Open("db_state", cfg)

	defer pudge.CloseAll()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		switch r.Method {
		case "POST":
			processPOSTMethod(w, r)
		case "GET":
			processGETMethod(w, r)
		}
	})

	http.ListenAndServe(":80", nil)

}

type IncommingMessage struct {
	Queue   string
	Message string
}

type Info struct {
	LastId uint64
}

func processPOSTMethod(w http.ResponseWriter, r *http.Request) {

	body, _ := io.ReadAll(r.Body)

	var incommingMessage IncommingMessage
	json.Unmarshal([]byte(body), &incommingMessage)

	queue_name := incommingMessage.Queue

	//cfg := &pudge.Config{
	//  SyncInterval: 1} // every second fsync
	//db_state, _ := pudge.Open(queue_name+"_state", cfg)

	/*
	   var dat map[string]interface{}
	   if err := json.Unmarshal(body, &dat); err != nil {
	       panic(err)
	   }
	*/

	var position uint64
	pudge.Get(queue_name+"_state", "post_position", &position)
	if position == 0 {
		position = 1
	}
	pudge.Set(queue_name, position, incommingMessage.Message)

	pudge.Set(queue_name+"_state", "post_position", position+1)

	fmt.Printf("Put to %s - %d\n", queue_name, position)

	//fmt.Fprintf(w, fmt.Sprint(position))
	res := Info{LastId: position}

	json_data, _ := json.Marshal(res)
	fmt.Fprint(w, string(json_data))
}

type OutgoingMessage struct {
	//Queue   string
	Id  uint64
	Msg string
}

func processGETMethod(w http.ResponseWriter, r *http.Request) {

	values := r.URL.Query()

	queue_name := values["queue"][0]
	after, _ := strconv.ParseInt(values["after"][0], 10, 64)
	limit, _ := strconv.Atoi(values["limit"][0])

	//cfg := &pudge.Config{
	//  SyncInterval: 1} // every second fsync
	//db_state, _ := pudge.Open(queue_name+"_state", cfg)

	var put_position uint64
	pudge.Get(queue_name+"_state", "post_position", &put_position)
	if put_position == 0 {
		// такой очереди нет, ничего не делаем больше
		return
	}

	//var id uint64

	keys, _ := pudge.Keys(queue_name, after, limit, 0, true)

	var res []OutgoingMessage

	//id = 0
	for _, key := range keys {
		id := binary.BigEndian.Uint64(key)
		output := ""
		pudge.Get(queue_name, key, &output)

		var message = OutgoingMessage{Id: id, Msg: output}

		res = append(res, message)

	}

	//w.Header().Add("id", fmt.Sprint(id))

	json_data, _ := json.Marshal(res)
	fmt.Fprint(w, string(json_data))
}
