package main

import (

	"github.com/go-pg/pg"
	"time"
	"math/rand"
	"fmt"
)

type Device struct {
	Id int
	Name string
	UserId int
}

type Metric struct {
	Id         int
	Deviceid   int
	Metric     [5]int
	LocalTime  time.Time
	ServerTime time.Time
}

var DB *pg.DB
var Dev = make(chan Device)


func init() {

	DB = pg.Connect(&pg.Options{
		User: "postgres",
		Password: "gfhjkm",
		Database: "dm",
		Addr:"localhost:5432",
	})



}


func getAllDevices() {

	//out := make(chan Device)
	for i:=1; i<10000; i++ {
		go func(i int) {
			var newD Device
			_, err := DB.QueryOne(pg.Scan(&newD.Id, &newD.Name, &newD.UserId), "SELECT * from devices where id = ?", i)
			if err != nil {
				//panic(err)
			}
			//log.Println(newD)
			Dev <- newD
		}(i)
	}

}

func createMetrics(in Device) {

	var newM Metric
	newM.Id = getLastID("device_metrics")
	for i:=0; i<len(newM.Metric); i++ {
		newM.Metric[i] = rand.Intn(100)
	}
	newM.Deviceid = in.Id
	newM.LocalTime = time.Now()
	newM.ServerTime = time.Now()
	fmt.Println(newM)
	time.Sleep(time.Second*5)
}

func getLastID(TableName string) int {
	var lastID int
	_, err := DB.QueryOne(pg.Scan(&lastID), "SELECT count(ID) + 1 from ?", TableName)
	if err != nil {
		//panic(err)
	}

	return lastID
}

func main() {

	for {
		getAllDevices()
		go createMetrics(<-Dev)
		time.Sleep(time.Second*1)
	}



}