package main

import (
	"log"
	"fmt"
	"net"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
  "gopkg.in/mgo.v2/bson"
)

func main() {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{
		Port: 5683,
		IP:   net.ParseIP("0.0.0.0"),
	})
	if err != nil {
		panic(err)
	}

	defer conn.Close()
	fmt.Printf("UDP protocol listening %s\n", conn.LocalAddr().String())

	for {
		message := make([]byte, 512) /* Set max msg at 512 bytes */
		rlen, remote, err := conn.ReadFromUDP(message[:])
		if err != nil {
			panic(err)
		}

		/* Received udp message */
		data := strings.TrimSpace(string(message[:rlen]))
		fmt.Printf("received: %s from %s\n", data, remote)

		/* Split string */
		splited_arr := strings.Split(data, ":")
		fmt.Printf("splited: %q\n", splited_arr)

		/* Write data to MongoDB */
		writedata_iot(splited_arr)

		/* Send back to NBIoT */
		_, err = conn.WriteToUDP([]byte("OK"), remote)
		if err != nil{
			fmt.Printf("Cannot send response to NB-IoT\n")
		}else {
			fmt.Printf("record completed !\n")
		}

	}
}

/* - Write data from IoT device into MongoDB database - */
func writedata_iot(_splited_arr []string)  {
  
  data0 := _splited_arr[0]
  data1 := _splited_arr[1]
  data2 := _splited_arr[2]
  
  fmt.Printf("data0 = %s\n", data0)
  fmt.Printf("data1 = %s\n", data1)
  fmt.Printf("data2 = %s\n", data2)

  t := int64(time.Now().Unix())
  now := time.Now()
  tm := now.Format(time.RFC3339)

  session, err := mgo.Dial("127.0.0.1:27017")
  if err != nil{
    log.Fatal(err)
    return
  }
  defer session.Close()

  collection := session.DB("udpDBbyGo").C("data")
  err = collection.Insert(bson.M{"data0":data0,"data1":data1,"data2":data2,"ts":t})
  if err != nil{
    log.Fatal(err)
    return
  }
	/* Record completed */
  fmt.Printf("record completed on %s\n", tm)
}
