package main

import (
	"time"
	"database/sql"
	"math/rand"
	"fmt"
	//"log"
	"strconv"
	_ "github.com/lib/pq"
)

//devices
type DevicesStruct struct {
	Id     int
	Name   string
	Userid int
}

//metrics
type DevicesMetricStruct struct {
	Id         int
	Deviceid   int
	Metric     [5]int
	LocalTime  time.Time
	ServerTime time.Time
}

//alerts
type DeviceAlertStruct struct {
	Id       int
	Deviceid int
	Message  string
}

var DB *sql.DB
var dbUser = "postgres"
var dbPass = "postgres"
var dbName = "postgres"

func init() {

	var err error
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", dbUser, dbPass, dbName)
	DB, err = sql.Open("postgres", dbinfo)
	if err != nil {
		panic(err)
	}
	//defer DB.Close() //because connection online - bad but for example

}

//clean DB with current tables- dmclean.sql PG 10
func GetAllDevicesFromDB() chan DevicesStruct {

	out := make(chan DevicesStruct)
	rows, err := DB.Query("SELECT * FROM devices")
	if err != nil {
		//panic(err)
	}
	defer rows.Close()

	go func() {
		for rows.Next() {
			var newDevice DevicesStruct
			err := rows.Scan(&newDevice.Id, &newDevice.Name, &newDevice.Userid)
			if err != nil {
				//panic(err)
			}
			out <- newDevice
		}
		close(out)
	}()

	return out
}

func CreateMetric(in chan DevicesStruct) chan DevicesMetricStruct {

	//create metrics every 5 sec
	//or maybe this value need in main function

	time.Sleep(5 * time.Second)
	out := make(chan DevicesMetricStruct)

	go func() {
		var newMetric DevicesMetricStruct
		for v := range in {
			//get uniq ID before write to DB
			newMetric.Id = TableIDs("device_metrics")
			newMetric.Deviceid = v.Id
			//set random metrics values
			for i := 0; i < len(newMetric.Metric); i++ {
				newMetric.Metric[i] = rand.Intn(50)
			}
			newMetric.LocalTime = time.Now().AddDate(0, 0, -1)
			newMetric.ServerTime = time.Now()
			//log.Println(newMetric)

			//insert new metric to DB
			var stringQ = "INSERT INTO device_metrics (Id, device_Id, metric_1, metric_2, metric_3, metric_4, metric_5, local_time, server_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
			_, err := DB.Exec(stringQ,
				newMetric.Id,
				newMetric.Deviceid,
				newMetric.Metric[0],
				newMetric.Metric[1],
				newMetric.Metric[2],
				newMetric.Metric[3],
				newMetric.Metric[4],
				newMetric.LocalTime,
				newMetric.ServerTime)
			if err != nil {
				fmt.Println(err.Error())
				//return
			}
			out <- newMetric
		}
		close(out)
	}()
	return out
}

//check metrics
func checkMetrics(in chan DevicesMetricStruct) {

	go func() {
		var newAlert DeviceAlertStruct
		for v := range in {
			for i := 0; i < len(v.Metric); i++ {
				//if one vlue from metric bad - create alert
				if v.Metric[i] == 43 {
					//get unique ID to write in DB
					newAlert.Id = TableIDs("device_alerts")
					newAlert.Deviceid = v.Deviceid
					newAlert.Message = "Bad metric param on device " + strconv.Itoa(v.Deviceid)

					//insert alert in DB
					_, err := DB.Exec("INSERT INTO device_alerts (id, device_id, message) VALUES ($1, $2, $3)", newAlert.Id, newAlert.Deviceid, newAlert.Message)
					if err != nil {
						fmt.Println(err.Error())
						//return
					}
				}
			}
		}
	}()
}

//get unique ID for new Row in table
func TableIDs(nameT string) (lastID int) {
	stringQ := "SELECT COUNT(ID) FROM " + nameT + ";"
	rows, err := DB.Query(stringQ)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&lastID)
		if err != nil {
			fmt.Println(err.Error())
			//return
		}
	}
	if err = rows.Err(); err != nil {
		fmt.Println(err.Error())
	}

	lastID++
	return lastID
}


func main() {

	//how do all of this correctly and parallel without changes logic for all program?
	//thanks!!!

	allDevices := GetAllDevicesFromDB()
	for {
		allMetrics := CreateMetric(allDevices)
		checkMetrics(allMetrics)
	}

}