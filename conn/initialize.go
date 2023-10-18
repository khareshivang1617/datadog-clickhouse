package conn

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func ClickhouseConnWithDatadogTracer(options *clickhouse.Options) (driver.Conn, error) {
	var err error
	span := ConnOpenSpan()
	defer FinishSpan(span, err)
	chDriverConn, err := clickhouse.Open(options)
	return &ClickhouseConnection{Conn: chDriverConn}, err
}
