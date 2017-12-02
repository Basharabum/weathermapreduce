import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureMapper  extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String year = line.substring(15, 19);
        String latitude = line.substring(28, 34);
        String longitude = line.substring(34, 41);

        int temperature = (line.charAt(87) == '+') ?
                Integer.parseInt(line.substring(88, 92)) :
                Integer.parseInt(line.substring(87, 92));

        String quality = line.substring(92, 93);

        //System.out.println(year + "x" + latitude + "y" + longitude + " temp : " + temperature/10);

        if (temperature != MISSING && quality.matches("[01459]")) {
            context.write(new Text(year + "x" + latitude + "y" + longitude), new IntWritable(temperature));
        }
    }
}
