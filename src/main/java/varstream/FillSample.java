package varstream;


import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static java.time.temporal.ChronoUnit.*;

public class FillSample {
    @FunctionHint(output = @DataTypeHint("ROW<sample_time TIMESTAMP(3)>"))
    private static class BaseFunction extends TableFunction<Row> {
        // TODO: INTERVAL type UDF params not supported in FlinkSQL / Calcite yet (CALCITE-4504)
        public void eval(LocalDateTime startTime, LocalDateTime endTime, LocalDateTime baseTime, Duration step) {
            if (step == null || step.isZero() || step.isNegative())
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

        void evalForTimeUnit(LocalDateTime startTime, LocalDateTime endTime, ChronoUnit timeUnit, int frequency) {
            eval(startTime, endTime, startTime.truncatedTo(timeUnit),
                    Duration.ofSeconds(timeUnit.getDuration().getSeconds() / frequency)) ;
        }
    }

    public static class PerDayFunction extends BaseFunction {
        public void eval(LocalDateTime startTime, LocalDateTime endTime, int frequency) {
            evalForTimeUnit(startTime, endTime, DAYS, frequency);
        }
    }

    public static class PerHourFunction extends BaseFunction {
        public void eval(LocalDateTime startTime, LocalDateTime endTime, int frequency) {
            evalForTimeUnit(startTime, endTime, HOURS, frequency);
        }
    }

    public static class PerMinuteFunction extends BaseFunction {
        public void eval(LocalDateTime startTime, LocalDateTime endTime, int frequency) {
            evalForTimeUnit(startTime, endTime, MINUTES, frequency);
        }
    }

    public static class PerSecondFunction extends BaseFunction {
        private final static long NANOS_PER_SECOND = 1000 * 1000 * 1000 ;

        public void eval(LocalDateTime startTime, LocalDateTime endTime, int frequency) {
            eval(startTime, endTime, startTime.truncatedTo(SECONDS), Duration.ofNanos(NANOS_PER_SECOND / frequency));
        }
    }
}
