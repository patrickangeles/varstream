package varstream;


import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static java.time.temporal.ChronoUnit.*;

/**
 * Keeps previous row time in a private static variable.
 * Probably not safe to use this in production
 */
@FunctionHint (output = @DataTypeHint("ROW<delay BIGINT>"))
public class ReplayAfterFunction extends TableFunction<Row> {

    private static LocalDateTime startTime = null ;

    @FunctionHint (input = {@DataTypeHint("INT"), @DataTypeHint("TIMESTAMP(3)")})
    public void eval(Integer minutes, LocalDateTime rowTime) {
        if (rowTime == null) {
            return ;
        }

        if (startTime == null) {
            startTime = rowTime.plusMinutes(minutes) ;
        }

        if (rowTime.isBefore(startTime)) {
            return ;
        }

        try {
            long duration = Duration.between (startTime, rowTime).toMillis() ;
            Thread.sleep (duration) ;
            collect (Row.of (duration)) ;
        }
        catch (InterruptedException e) {
            // do nuffin
        }
        startTime = rowTime ;
    }
}
