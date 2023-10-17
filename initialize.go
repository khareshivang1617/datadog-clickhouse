package main

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func ClickhouseConnWithDatadogTracer(options *clickhouse.Options) (driver.Conn, error) {
	span := ConnOpenSpan()
	defer span.Finish()
	chDriverConn, err := clickhouse.Open(options)
	return &ClickhouseConnection{Conn: chDriverConn}, err
}
