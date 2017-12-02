import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureReducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        int maxValue = Integer.MIN_VALUE;
        int minValue = Integer.MAX_VALUE;

        for (IntWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
            minValue = Math.min(minValue, value.get());
        }

        context.write(key, new Text("[" + Integer.toString(maxValue) + "/" + Integer.toString(minValue) + "]"));
    }
}
