package main

import (
	"caldavsms"
	"time"
)

const (
	username     = "XXX"
	password     = "XXX"
	uri          = "http://XXX.XXX.XXX.XXX:8080/baikal/html/dav.php"
	calendarname = "XXX"
	location     = "Europe/Moscow"
	storagename  = "tmp-caldavsms"
	firsttoken   = "http://sabre.io/ns/sync/0"
)

func main() {
	loc, err := time.LoadLocation(location)
	if err != nil {
		panic(err)
	}
	var mintime = time.Date(2024, time.Month(1), 1, 0, 0, 0, 0, loc)
	caldavsms.Sync(username, password, uri, calendarname, location, storagename, firsttoken, mintime)
}
