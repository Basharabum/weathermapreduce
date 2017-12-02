import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureReducerTotal extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {

        int maxValue = Integer.MIN_VALUE;
        int minValue = Integer.MAX_VALUE;

        for (Text value : values) {
            String line = value.toString();

            int tempSeparatorIndex = line.indexOf('/');

            int minTemperature = Integer.parseInt(line.substring(0, tempSeparatorIndex));
            int maxTemperature = Integer.parseInt(line.substring(tempSeparatorIndex + 1));

            maxValue = Math.max(maxValue, maxTemperature);
            minValue = Math.min(minValue, minTemperature);
        }

        context.write(key, new Text("Max value: " + Integer.toString(maxValue) + " | Min value: " + Integer.toString(minValue)));
    }
}
