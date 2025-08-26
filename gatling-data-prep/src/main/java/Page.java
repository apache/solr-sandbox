public class Page {
  String title;
  String id;
  String contributor;
  String date;
  String text;
  String comment;

  @Override
  public String toString() {
    return "Page [title=" + title + ", id=" + id + ", contributor=" + contributor + ", date="
            + date + ", text=" + text + ", comment=" + comment + "]";
  }

  public String[][] toFields() {
    String[][] fields = new String[6][2];
    fields[0] = new String[]{"id", id};
    fields[1] = new String[]{"title", title};
    fields[2] = new String[]{"date", date};
    fields[3] = new String[]{"contributor", contributor};
    fields[4] = new String[]{"comment", comment};
    fields[5] = new String[]{"text", text};
    return fields;
  }
}
