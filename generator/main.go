package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	duckdb "github.com/marcboeker/go-duckdb"
)

const (
	CBD_FACTOR              float32 = 5.1
	POWER_TO_COST           float32 = 3.2
	NUMBER_OF_WEATHER_TYPES int     = 4
)

type Fact struct {
	id          int64
	time_id     int64
	date_id     int64
	location_id int64
	cons        float32
	cost        float32
}

type Time struct {
	time string
	id   int64
}

type Date struct {
	date    time.Time
	weather Weather
	id      int64
}

type Weather struct {
	label        string
	id           int64
	solar_factor float32
	cons_factor  float32
}

type Location struct {
	name         string
	id           int64
	population   int64
	is_cbd       bool
	solar_factor float32
}

func dim_location() []Location {
	return []Location{
		{name: "Brisbane", id: 0, population: 10000, is_cbd: true, solar_factor: 0.6},
		{name: "Morningside", id: 1, population: 11200, is_cbd: false, solar_factor: 0.7},
		{name: "Sunnyback", id: 2, population: 9000, is_cbd: false, solar_factor: 0.5},
		{name: "Carseldine", id: 3, population: 10000, is_cbd: false, solar_factor: 0.8},
		{name: "Toowong", id: 4, population: 12500, is_cbd: false, solar_factor: 0.9},
	}
}

func dim_time() []Time {
	var res []Time
	t := time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 1440; i++ {
		res = append(res, Time{time: t.Format(time.TimeOnly), id: int64(i)})
		t = t.Add(time.Minute)
	}
	return res
}

func generate_weather_for_date(d int) Weather {
	switch day := d % NUMBER_OF_WEATHER_TYPES; day {
	case 1:
		return Weather{label: "cloudy", id: 0, solar_factor: 0.3, cons_factor: 0.7}
	case 2:
		return Weather{label: "rainy", id: 2, solar_factor: 0.05, cons_factor: 0.7}
	case 3:
		return Weather{label: "windy", id: 3, solar_factor: 0.7, cons_factor: 0.7}
	default: // 0 && > 3
		return Weather{label: "sunny", id: 1, solar_factor: 1, cons_factor: 1}
	}
}

func dim_date() []Date {
	var res []Date
	d := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 365; i++ {
		date := Date{date: d, id: int64(i), weather: generate_weather_for_date(i)}
		res = append(res, date)
		d = d.AddDate(0, 0, 1)
	}
	return res
}

func generate(date []Date, time []Time, locations []Location) []Fact {
	var res []Fact
	var row_count int64 = 0

	for _, d := range date {
		for _, t := range time {
			for _, l := range locations {
				row := Fact{
					id:          row_count,
					time_id:     t.id,
					date_id:     d.id,
					location_id: l.id,
					cons:        model_consumption(t, d, l),
					cost:        model_cost(t, d, l),
				}
				res = append(res, row)
				row_count++
			}
		}
	}

	return res
}

func model_consumption(t Time, d Date, l Location) float32 {
	// base
	pop := float32(l.population)
	base := float32(rand.NormFloat64()*float64(pop)) + pop

	// cbd factor
	if l.is_cbd {
		base = CBD_FACTOR * base
	}

	// weather
	base += base * d.weather.cons_factor

	// solar
	if t.id > 360 && t.id < 1080 {
		solar_arc := float32(t.id-360) * float32(1080-t.id) / (360 * 360)
		base -= base * l.solar_factor * solar_arc * d.weather.solar_factor
	}

	return base
}

func model_cost(t Time, d Date, l Location) float32 {
	// base
	pop := float32(l.population)
	base := float32(rand.NormFloat64()*float64(pop)) + pop

	// cost adj
	base = base / POWER_TO_COST

	// cbd factor
	if l.is_cbd {
		base = CBD_FACTOR * base
	}

	// weather
	base += base * d.weather.cons_factor

	// solar
	if t.id > 360 && t.id < 1080 {
		solar_arc := float32(t.id-360) * float32(1080-t.id) / (360 * 360)
		base -= base * l.solar_factor * solar_arc * d.weather.solar_factor
	}

	return base
}

func create_dim_date(connector *duckdb.Connector) ([]Date, error) {
	// create table
	db := sql.OpenDB(connector)

	_, err := db.Exec(`CREATE OR REPLACE TABLE dim_date (
    id BIGINT,
    weather_id BIGINT,
    date DATETIME
  )`)
	if err != nil {
		return []Date{}, err
	}

	// insert rows
	conn, err := connector.Connect(context.Background())
	if err != nil {
		return []Date{}, err
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "dim_date")
	if err != nil {
		return []Date{}, err
	}
	defer appender.Close()

	dates := dim_date()
	for _, date := range dates {
		err = appender.AppendRow(date.id, date.weather.id, date.date)
		if err != nil {
			return []Date{}, err
		}
	}

	return dates, nil
}

func create_dim_time(connector *duckdb.Connector) ([]Time, error) {
	// create table
	db := sql.OpenDB(connector)

	_, err := db.Exec(`CREATE OR REPLACE TABLE dim_time (id BIGINT, time TIME)`)
	if err != nil {
		return []Time{}, err
	}

	stmt, err := db.PrepareContext(context.Background(), "INSERT INTO dim_time VALUES (?, ?)")
	if err != nil {
		return []Time{}, err
	}

	times := dim_time()
	for _, time := range times {
		stmt.Exec(time.id, time.time)
	}

	return times, nil
}

func create_dim_location(connector *duckdb.Connector) ([]Location, error) {
	// create table
	db := sql.OpenDB(connector)

	_, err := db.Exec(`CREATE OR REPLACE TABLE dim_location (
    id BIGINT,
    name VARCHAR,
    population BIGINT,
    is_cbd BOOL,
    solar_factor FLOAT
  )`)
	if err != nil {
		return []Location{}, err
	}

	// insert rows
	conn, err := connector.Connect(context.Background())
	if err != nil {
		return []Location{}, err
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "dim_location")
	if err != nil {
		return []Location{}, err
	}
	defer appender.Close()

	locations := dim_location()
	for _, location := range locations {
		err = appender.AppendRow(location.id, location.name, location.population, location.is_cbd, location.solar_factor)
		if err != nil {
			return []Location{}, err
		}
	}

	return locations, nil
}

func create_dim_weather(connector *duckdb.Connector) error {
	// create table
	db := sql.OpenDB(connector)

	_, err := db.Exec(`CREATE OR REPLACE TABLE dim_weather (
    id BIGINT,
    label VARCHAR,
    solar_factor FLOAT,
    cons_factor FLOAT
  )`)
	if err != nil {
		return err
	}

	// insert rows
	conn, err := connector.Connect(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "dim_weather")
	if err != nil {
		return err
	}
	defer appender.Close()

	for i := 0; i < NUMBER_OF_WEATHER_TYPES; i++ {
		w := generate_weather_for_date(i)
		err = appender.AppendRow(w.id, w.label, w.solar_factor, w.cons_factor)
		if err != nil {
			return err
		}
	}

	return nil
}

func create_fact_energy(connector *duckdb.Connector, dates []Date, times []Time, locations []Location) error {
	// create table
	db := sql.OpenDB(connector)

	_, err := db.Exec(`CREATE OR REPLACE TABLE fact_energy (
    id BIGINT,
    date_id BIGINT,
    time_id BIGINT,
    location_id BIGINT,
    cons FLOAT,
    cost FLOAT
  )`)
	if err != nil {
		return err
	}

	// insert rows
	conn, err := connector.Connect(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "fact_energy")
	if err != nil {
		return err
	}
	defer appender.Close()

	facts := generate(dates, times, locations)
	for _, fact := range facts {
		err = appender.AppendRow(fact.id, fact.date_id, fact.time_id, fact.location_id, fact.cons, fact.cost)
		if err != nil {
			return err
		}
	}

	return nil
}

func create_mart_energy(connector *duckdb.Connector) error {
	db := sql.OpenDB(connector)

	_, err := db.Exec(`CREATE OR REPLACE TABLE mart_energy AS 
        SELECT
            date.date AS date,
            weather.label AS weather,
            time.time AS time,
            location.name AS location,
            location.population AS population,
            energy.cons AS consumption,
            energy.cost AS cost
        FROM fact_energy AS energy
        INNER JOIN dim_date AS date ON energy.date_id = date.id
        INNER JOIN dim_weather AS weather on date.weather_id = weather.id
        INNER JOIN dim_time AS time on energy.time_id = time.id
        INNER JOIN dim_location AS location ON energy.location_id = location.id`)
	return err
}

func export_mart_energy(connector *duckdb.Connector) error {
	db := sql.OpenDB(connector)
	_, err := db.Exec(`COPY mart_energy TO '../mart_energy.parquet' (FORMAT PARQUET);`)
	return err
}

func write_to_db() {
	// nuke db
	err := os.Remove("../data.db")
	if err != nil {
		fmt.Println("Creating new at data.db")
	} else {
		fmt.Println("Overwriting existing data.db")
	}
	os.Remove("../data.db.wal")

	// connect to db
	connector, err := duckdb.NewConnector("../data.db", nil)
	if err != nil {
		panic("failed to create duckdb connector")
	}
	defer connector.Close()

	// dim date
	dates, err := create_dim_date(connector)
	if err != nil {
		log.Fatalf("error while creating dim_date: %s", err)
	}

	// dim time
	times, err := create_dim_time(connector)
	if err != nil {
		log.Fatalf("error while creating dim_time: %s", err)
	}

	// dim location
	locations, err := create_dim_location(connector)
	if err != nil {
		log.Fatalf("error while creating dim_location: %s", err)
	}

	// dim weather
	err = create_dim_weather(connector)
	if err != nil {
		log.Fatalf("error while creating dim_weather: %s", err)
	}

	// fact energy
	err = create_fact_energy(connector, dates, times, locations)
	if err != nil {
		log.Fatalf("error while creating fact_energy: %s", err)
	}

	// mart energy
	err = create_mart_energy(connector)
	if err != nil {
		log.Fatalf("error while creating mart_energy: %s", err)
	}

	// export mart energy
	err = export_mart_energy(connector)
	if err != nil {
		log.Fatalf("error while exporting mart_energy: %s", err)
	}
}

func main() {
	fmt.Println("Creating DuckDB database!")
	write_to_db()
	fmt.Println("Finished writing data.db")
}
