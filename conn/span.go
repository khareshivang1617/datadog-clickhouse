package conn

import (
	"context"

	"github.com/khareshivang1617/datadog-clickhouse/constants"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func StartSpanForQuery(ctx context.Context, query string) ddtrace.Span {
	span, _ := tracer.StartSpanFromContext(
		ctx,
		constants.DDTagQueryOperationName,
		tracer.Tag(constants.DDTagDBStatement, query),
		tracer.ResourceName(query),
		tracer.Measured(),
	)
	return span
}

func ConnOpenSpan() ddtrace.Span {
	span := tracer.StartSpan(
		constants.DDTagQueryOperationName,
		tracer.ResourceName("Connect"),
		tracer.Tag(constants.DDTagDBStatement, "Connect"),
		tracer.Tag(constants.DDTagSqlQueryType, "Connect"),
		tracer.Measured(),
	)
	return span
}

func ConnCloseSpan() ddtrace.Span {
	span := tracer.StartSpan(
		constants.DDTagQueryOperationName,
		tracer.ResourceName("Close"),
		tracer.Tag(constants.DDTagDBStatement, "Close"),
		tracer.Tag(constants.DDTagSqlQueryType, "Close"),
		tracer.Measured(),
	)
	return span
}

func PingSpan(ctx context.Context) ddtrace.Span {
	span, _ := tracer.StartSpanFromContext(
		ctx,
		constants.DDTagQueryOperationName,
		tracer.ResourceName("Ping"),
		tracer.Tag(constants.DDTagDBStatement, "Ping"),
		tracer.Tag(constants.DDTagSqlQueryType, "Ping"),
		tracer.Measured(),
	)
	return span
}

func FinishSpan(span ddtrace.Span, err error) {
	span.Finish(func(cfg *ddtrace.FinishConfig) {
		cfg.Error = err
	})
}
