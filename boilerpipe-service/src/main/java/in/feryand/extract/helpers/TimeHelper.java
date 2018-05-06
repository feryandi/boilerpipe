package in.feryand.extract.helpers;

import in.feryand.extract.objects.SimpleDate;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class TimeHelper {

  private static TimeHelper singleton = new TimeHelper();

  private TimeHelper() { }

  public static TimeHelper getInstance( ) {
    return singleton;
  }

  public Calendar getCalendar(String dateString) throws ParseException {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date = df.parse(dateString);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(date);

    return calendar;
  }

  public String getTimestamp(String dateString) throws ParseException {
    return Long.toString(getCalendar(dateString).getTimeInMillis());
  }

  public String getDatePath(String dateString) throws ParseException {
    Calendar calendar = getCalendar(dateString);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH) + 1;
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    int hour = calendar.get(Calendar.HOUR_OF_DAY);

    return year + "/" +
        String.format("%02d", month) + "/" +
        String.format("%02d", day) + "/" +
        String.format("%02d", hour);
  }

  public SimpleDate getDate(String dateString) throws ParseException {
    SimpleDate result = new SimpleDate();
    Calendar calendar = getCalendar(dateString);

    result.year = Integer.toString(calendar.get(Calendar.YEAR));
    result.month = String.format("%02d", calendar.get(Calendar.MONTH) + 1);
    result.day = String.format("%02d", calendar.get(Calendar.DAY_OF_MONTH));
    result.hour = String.format("%02d", calendar.get(Calendar.HOUR_OF_DAY));

    return result;
  }

}
