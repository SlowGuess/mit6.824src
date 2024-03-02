package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func HelloWorld(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello World!!!!!")
}

func main() {
	go func() {
		http.HandleFunc("/", HelloWorld)
		http.ListenAndServe(":8080", nil)
	}()

	resp, err := http.Get("http://localhost:8080/")
	if err != nil {
		// 处理错误
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// 处理错误
	}
	fmt.Println(string(body))
}
