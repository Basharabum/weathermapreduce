import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureMapperTotal  extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        int tempStartIndex = line.indexOf('[');
        int tempSeparatorIndex = line.indexOf('/');
        int tempEndIndex = line.indexOf('[');

        int maxTemperature = Integer.parseInt(line.substring(tempStartIndex + 1, tempSeparatorIndex));
        int minTemperature = Integer.parseInt(line.substring(tempSeparatorIndex + 1, tempEndIndex));

        //System.out.println(year + "x" + latitude + "y" + longitude + " temp : " + temperature/10);
        //Отправить min и max как строку и распарсить внутри reducer'a
        context.write(new Text("result"), new Text(Integer.toString(maxTemperature) + "/" + Integer.toString(minTemperature)));
    }
}
