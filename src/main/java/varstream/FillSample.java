package varstream;


import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.LocalDateTime;

import static java.time.temporal.ChronoUnit.*;

public class FillTimestamp {
    private static long SECONDS_PER_MINUTE = 60;
    private static long SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
    private static long SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;

    @FunctionHint(output = @DataTypeHint("ROW<ts TIMESTAMP(3)>"))
    private class BaseFunction extends TableFunction<Row> {
        // TODO: INTERVAL type not supported in FlinkSQL / Calcite yet...
        public void eval(LocalDateTime startTime, LocalDateTime endTime, LocalDateTime baseTime, Duration step) {
            if (step.isZero() || step.isNegative())
                return;

            LocalDateTime current = baseTime;

            while (current.isBefore(startTime)) {
                current = current.plus(step);
            }

            while (current.isBefore(endTime)) {
                collect(Row.of(current));
                current = current.plus(step);
            }
        }
    }

    public static class PerDayFunction extends BaseFunction {
        public void eval(LocalDateTime startTime, LocalDateTime endTime, int slices) {
            eval(startTime, endTime, startTime.truncatedTo(DAYS), Duration.ofSeconds(SECONDS_PER_DAY / slices)));
        }
    }

    public static class PerHourFunction extends BaseFunction {
        public void eval(LocalDateTime startTime, LocalDateTime endTime, int slices) {
            eval(startTime, endTime, startTime.truncatedTo(HOURS), Duration.ofSeconds(SECONDS_PER_HOUR / slices)));
        }
    }

    public static class PerMinuteFunction extends BaseFunction {
        public void eval(LocalDateTime startTime, LocalDateTime endTime, int slices) {
            eval(startTime, endTime, startTime.truncatedTo(MINUTES), Duration.ofSeconds(SECONDS_PER_MINUTE / slices)));
        }
    }

    public static class PerSecondFunction extends BaseFunction {
        public void eval(LocalDateTime startTime, LocalDateTime endTime, int slices) {
            eval(startTime, endTime, startTime.truncatedTo(SECONDS), Duration.ofNanos(SECONDS_PER_MINUTE / slices)));
        }
    }
}