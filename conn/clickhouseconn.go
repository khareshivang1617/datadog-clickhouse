package conn

import (
	"context"

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

func (ch *ClickhouseConnection) Select(ctx context.Context, dest any, query string, args ...any) (err error) {
	span := StartSpanForQuery(ctx, query)
	defer FinishSpan(span, err)
	err = ch.Conn.Select(ctx, dest, query, args...)
	return
}

func (ch *ClickhouseConnection) Query(ctx context.Context, query string, args ...any) (rows driver.Rows, err error) {
	span := StartSpanForQuery(ctx, query)
	defer FinishSpan(span, err)
	rows, err = ch.Conn.Query(ctx, query, args...)
	return
}

func (ch *ClickhouseConnection) QueryRow(ctx context.Context, query string, args ...any) (row driver.Row) {
	span := StartSpanForQuery(ctx, query)
	defer func() {
		if row != nil {
			FinishSpan(span, row.Err())
		} else {
			FinishSpan(span, nil)
		}
	}()

	row = ch.Conn.QueryRow(ctx, query, args...)
	return
}

func (ch *ClickhouseConnection) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return ch.Conn.PrepareBatch(ctx, query, opts...)
}

func (ch *ClickhouseConnection) Exec(ctx context.Context, query string, args ...any) (err error) {
	span := StartSpanForQuery(ctx, query)
	defer FinishSpan(span, err)
	err = ch.Conn.Exec(ctx, query, args...)
	return
}

func (ch *ClickhouseConnection) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return ch.Conn.AsyncInsert(ctx, query, wait, args...)
}

func (ch *ClickhouseConnection) Ping(ctx context.Context) (err error) {
	span := PingSpan(ctx)
	defer FinishSpan(span, err)
	err = ch.Conn.Ping(ctx)
	return
}

func (ch *ClickhouseConnection) Stats() driver.Stats {
	return ch.Conn.Stats()
}

func (ch *ClickhouseConnection) Close() (err error) {
	span := ConnCloseSpan()
	defer FinishSpan(span, err)
	err = ch.Conn.Close()
	return
}
