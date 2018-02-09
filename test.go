package main

import (

	"github.com/go-pg/pg"
	"time"
	"math/rand"
)

type Device struct {
	Id int
	Name string
	UserId int
}

type Metric struct {
	Id         int
	Deviceid   int
	Metric     [5]int64
	LocalTime  time.Time
	ServerTime time.Time
}

var DB *pg.DB
var Dev = make(chan Device)
//var Metr = make(chan Metric)
var LastIDm int

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
	getLastID("device_metrics")
	newM.Id = LastIDm
	for i:=0; i<len(newM.Metric); i++ {
		newM.Metric[i] = rand.Int63n(100)
	}
	newM.Deviceid = in.Id
	newM.LocalTime = time.Now()
	newM.ServerTime = time.Now()
	//Metr <- newM
	//fmt.Println(newM)
	//insertMetricsDB(newM)
}

func insertMetricsDB(in Metric) {


	//log.Println(&in.Id, &in.Deviceid, &in.Metric[0], &in.Metric[1], &in.Metric[2], &in.Metric[3], &in.Metric[4], &in.LocalTime, &in.ServerTime)
	//_, err := DB.QueryOne(&in.Id, &in.Deviceid, &in.Metric[0], &in.Metric[1], &in.Metric[2], &in.Metric[3], &in.Metric[4], &in.LocalTime, &in.ServerTime,
	//	`INSERT INTO device_metrics (id, device_id, metric_1, metric_2, metric_3, metric_4, metric_5, local_time, server_time)
	//		VALUES (?id, ?device_id, ?metric_1, ?metric_2, ?metric_3, ?metric_4, ?metric_5, ?local_time, ?server_time)`,
	//	&in.Id, &in.Deviceid, &in.Metric[0], &in.Metric[1], &in.Metric[2], &in.Metric[3], &in.Metric[4], &in.LocalTime, &in.ServerTime)
	//if err != nil {
	//	//panic(err)
	//}
	var stringQ = "INSERT INTO device_metrics (Id, device_Id, metric_1, metric_2, metric_3, metric_4, metric_5, local_time, server_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
	_, err := DB.Exec(stringQ,
		in.Id,
		in.Deviceid,
		in.Metric[0],
		in.Metric[1],
		in.Metric[2],
		in.Metric[3],
		in.Metric[4],
		in.LocalTime,
		in.ServerTime)
	if err != nil {
		//fmt.Println(err.Error())
		//return
	}

}



func getLastID(TableName string)  {

	_, err := DB.QueryOne(pg.Scan(&LastIDm), "SELECT count(ID) from ?", TableName)
	if err != nil {
		//panic(err)
	}

	LastIDm++
}

func main() {

	for {
		go getAllDevices()
		go createMetrics(<-Dev)
		//go insertMetricsDB(<-Metr)
		time.Sleep(time.Second*1)

	}

}

