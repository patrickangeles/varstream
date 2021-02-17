package varstream;


import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;

import static java.time.temporal.ChronoUnit.MINUTES;

@FunctionHint(output = @DataTypeHint("ROW<ts TIMESTAMP(3)>"))
public class FillTimestampFunction extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(FillTimestampFunction.class);

    public void eval(LocalDateTime startTime, LocalDateTime endTime, Duration step) {

        if (step == null || step.isNegative())
            return ;

        if (step.)

        LocalDateTime current = startTime.truncatedTo(MINUTES);

        while (current.isBefore (startTime)) {
            current = current.plus(step);
        }

        while (current.isBefore(endTime)) {
            collect (Row.of(current));
            current = current.plus(step);
        }
    }

    public void eval(LocalDateTime startTime, LocalDateTime endTime, Long seconds) {
        eval (startTime,endTime, Duration.ofSeconds(seconds)) ;
    }
}
