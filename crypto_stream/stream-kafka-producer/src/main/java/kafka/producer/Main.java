package kafka.producer;

import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        System.out.println(1299038700000L);
        Date date = new Date(1438905600L);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String formattedDate = sdf.format(date);
        System.out.println("Formatted date: " + formattedDate);
        //1715089953749
        //1715084280197000
    }
}