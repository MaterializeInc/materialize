// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	regionSeedCount    = 10
	userSeedCount      = 100
	purchaseSeedCount  = 100
	purchaseGenCount   = 10000
	purchaseGenEveryMS = 100
)

func doExec(db *sql.DB, str string) {
	_, err := db.Exec(str)
	if err != nil {
		panic(err.Error())
	}
}

func genPrepare(db *sql.DB, str string) *sql.Stmt {
	prep, err := db.Prepare(str)
	if err != nil {
		panic(err.Error())
	}

	return prep
}

func main() {
	db, err := sql.Open("mysql", "root:debezium@tcp(mysql:3306)/")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	doExec(db, "CREATE DATABASE IF NOT EXISTS simple")

	// simple.region.time is a workaround for the fact that Materialize requires
	// some activity on row after source is created to ingest it. Will be
	// removed in future version.
	doExec(db, `CREATE TABLE IF NOT EXISTS simple.region
		(
			id SERIAL PRIMARY KEY,
			time TIMESTAMP
		);`)

	// simple.user.time is a workaround for the fact that Materialize requires
	// some activity on row after source is created to ingest it. Will be
	// removed in future version.
	doExec(db, `CREATE TABLE IF NOT EXISTS simple.user
		(
			id SERIAL PRIMARY KEY,
			region_id BIGINT UNSIGNED REFERENCES region(id),
			time TIMESTAMP
		);`)

	doExec(db, `CREATE TABLE IF NOT EXISTS simple.purchase
		(
			user_id BIGINT UNSIGNED REFERENCES user(id),
			amount DECIMAL(12,2)
		);`)

	insertRegion := genPrepare(db, "INSERT INTO simple.region VALUES ();")
	insertUser := genPrepare(db,
		"INSERT INTO simple.user (region_id) VALUES ( ? )")
	insertPurchase := genPrepare(db,
		"INSERT INTO simple.purchase (amount, user_id) VALUES ( ?, ? )")
	updateRegionTime := genPrepare(db, "UPDATE simple.region SET time = ?")
	updateUserTime := genPrepare(db, "UPDATE simple.user SET time = ?")

	rndSrc := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(rndSrc)

	// Seed regions.
	for i := 0; i < regionSeedCount; i++ {
		_, err = insertRegion.Exec()
		if err != nil {
			panic(err.Error())
		}
	}

	// Get range of region' IDs (min + count); expects IDs to be sequential,
	// which is reasonable using SERIAL with the load gens access patterns.
	var regionIDMin, regionIDCount int
	err = db.QueryRow("SELECT MIN(id) FROM simple.region;").Scan(&regionIDMin)
	if err != nil {
		panic(err.Error())
	}
	err = db.QueryRow("SELECT DISTINCT COUNT(*) FROM simple.region;").Scan(&regionIDCount)
	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("Inserted %d regions; %d regions total expected in report\n", regionSeedCount, regionIDCount)

	// Seed users.
	for i := 0; i < userSeedCount; i++ {
		// Insert user with random region.
		_, err = insertUser.Exec(rnd.Intn(regionIDCount) + regionIDMin)

		if err != nil {
			panic(err.Error())
		}
	}

	// Get range of user IDs (min + count); expects IDs to be sequential, which
	// is reasonable using SERIAL with the load gens access patterns.
	var userIDMin, userIDCount int
	err = db.QueryRow("SELECT MIN(id) FROM simple.user;").Scan(&userIDMin)
	if err != nil {
		panic(err.Error())
	}
	err = db.QueryRow("SELECT DISTINCT COUNT(*) FROM simple.user;").Scan(&userIDCount)
	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("Inserted %d users\n", userSeedCount)

	// Seed purchases.
	for i := 0; i < purchaseSeedCount; i++ {
		// Insert purchase with random amount and user.
		_, err = insertPurchase.Exec(rnd.Float64()*10000, rnd.Intn(userIDCount)+userIDMin)
		if err != nil {
			panic(err.Error())
		}
	}

	fmt.Printf("Inserted %d purchases\n", purchaseSeedCount)

	// Do some math to let users understand how fast data changes and how long
	// the loadgen is set to run.
	sleepTime := purchaseGenEveryMS * time.Millisecond
	purchaseGenPerSecond := time.Second / sleepTime
	purchaseGenPeriod := purchaseGenCount / (purchaseGenPerSecond * 60)

	fmt.Printf("Generating %d purchases (%d/s for %dm)\n",
		purchaseGenCount, purchaseGenPerSecond, purchaseGenPeriod)

	// Continue generating purchases.
	for i := 0; i < 10000; i++ {
		_, err = insertPurchase.Exec(rnd.Float64()*10000, rnd.Intn(userIDCount)+userIDMin)

		if err != nil {
			panic(err.Error())
		}

		// Materialize requires some activity on row after source is created to
		// ingest it. This frequency is overkill but was the simplest way to
		// ensure some activity occurs at some indeterminate point after the
		// loadgen starts.
		timeNow := time.Now()
		updateRegionTime.Exec(timeNow)
		updateUserTime.Exec(timeNow)
		time.Sleep(sleepTime)
	}

	fmt.Println("Done generating purchases. ttfn.")
}
