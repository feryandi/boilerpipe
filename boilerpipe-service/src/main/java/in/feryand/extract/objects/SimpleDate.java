package in.feryand.extract.objects;

public class SimpleDate {
  public String year;
  public String month;
  public String day;

  public SimpleDate() {
    this.year = "0000";
    this.month = "00";
    this.day = "00";
  }

  public SimpleDate(String year, String month, String day) {
    this.year = year;
    this.month = month;
    this.day = day;
  }
}