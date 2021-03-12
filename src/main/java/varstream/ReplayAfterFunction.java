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
 * Keeps state in a private static variable.
 * Probably not safe to use this in production
 */
@FunctionHint (output = @DataTypeHint("ROW<delay BIGINT>"))
public class ReplayAfterFunction extends TableFunction<Row> {

    private static Duration differenceFromNow = null ;

    @FunctionHint (input = {@DataTypeHint("INT"), @DataTypeHint("TIMESTAMP(3)")})
    public void eval(Integer minutes, LocalDateTime rowTime) {
        if (rowTime == null) {
            return ;
        }

        if (differenceFromNow == null) {
            // first time calling this function
            differenceFromNow = Duration.between(rowTime.plusMinutes(minutes), LocalDateTime.now()) ;
        }

        LocalDateTime adjustedRowTime = rowTime.plus(differenceFromNow) ;
        LocalDateTime now = LocalDateTime.now () ;

        if (adjustedRowTime.isBefore(now)) {
            return ;
        }

        try {
            // adjusted row time is later than now, so we wait
            long duration = Duration.between (now, adjustedRowTime).toMillis() ;
            Thread.sleep (duration) ;
            collect (Row.of (duration)) ;
        }
        catch (InterruptedException e) {
            // do nuffin
        }
    }
}
