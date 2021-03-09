package varstream;


import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static java.time.temporal.ChronoUnit.*;

@FunctionHint(output = @DataTypeHint("ROW<sample_time TIMESTAMP(3)>"))
public class Delay extends TableFunction<Row> {
    public void eval(LocalDateTime startTime, LocalDateTime rowTime) {
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
}
