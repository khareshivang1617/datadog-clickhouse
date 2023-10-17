package conn

import (
	"context"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func StartSpanForQuery(ctx context.Context, query string) ddtrace.Span {
	span, _ := tracer.StartSpanFromContext(
		ctx,
		"clickhouse.query",
		tracer.Tag("db.statement", query),
		tracer.ResourceName(query),
	)
	return span
}

func ConnOpenSpan() ddtrace.Span {
	span := tracer.StartSpan(
		"clickhouse.query",
		tracer.ResourceName("Connect"),
		tracer.Tag("db.statement", "Connect"),
		tracer.Tag("sql.query_type", "Connect"),
	)
	return span
}

func ConnCloseSpan() ddtrace.Span {
	span := tracer.StartSpan(
		"clickhouse.query",
		tracer.ResourceName("Close"),
		tracer.Tag("db.statement", "Close"),
		tracer.Tag("sql.query_type", "Close"),
	)
	return span
}

func PingSpan(ctx context.Context) ddtrace.Span {
	span, _ := tracer.StartSpanFromContext(
		ctx,
		"clickhouse.query",
		tracer.ResourceName("Ping"),
		tracer.Tag("db.statement", "Ping"),
		tracer.Tag("sql.query_type", "Ping"),
	)
	return span
}
