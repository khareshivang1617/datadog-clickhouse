package conn

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickhouseConnection struct {
	Conn driver.Conn
}

func (ch *ClickhouseConnection) Contributors() []string {
	return ch.Conn.Contributors()
}

func (ch *ClickhouseConnection) ServerVersion() (*driver.ServerVersion, error) {
	return ch.Conn.ServerVersion()
}

func (ch *ClickhouseConnection) Select(ctx context.Context, dest any, query string, args ...any) error {
	fmt.Println("SELECTQUERY: ", query)
	span := StartSpanForQuery(ctx, query)
	defer span.Finish()
	return ch.Conn.Select(ctx, dest, query, args...)
}

func (ch *ClickhouseConnection) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	span := StartSpanForQuery(ctx, query)
	defer span.Finish()
	return ch.Conn.Query(ctx, query, args...)
}

func (ch *ClickhouseConnection) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	span := StartSpanForQuery(ctx, query)
	defer span.Finish()
	return ch.Conn.QueryRow(ctx, query, args...)
}

func (ch *ClickhouseConnection) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return ch.Conn.PrepareBatch(ctx, query, opts...)
}

func (ch *ClickhouseConnection) Exec(ctx context.Context, query string, args ...any) error {
	span := StartSpanForQuery(ctx, query)
	defer span.Finish()
	return ch.Conn.Exec(ctx, query, args...)
}

func (ch *ClickhouseConnection) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return ch.Conn.AsyncInsert(ctx, query, wait, args...)
}

func (ch *ClickhouseConnection) Ping(ctx context.Context) error {
	span := PingSpan(ctx)
	defer span.Finish()
	return ch.Conn.Ping(ctx)
}

func (ch *ClickhouseConnection) Stats() driver.Stats {
	return ch.Conn.Stats()
}

func (ch *ClickhouseConnection) Close() error {
	span := ConnCloseSpan()
	defer span.Finish()
	return ch.Conn.Close()
}
