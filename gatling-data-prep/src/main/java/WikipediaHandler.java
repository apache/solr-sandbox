import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public abstract class WikipediaHandler extends DefaultHandler {
  private final static Set<String> interestingElements = Set.of("title", "id", "username", "comment", "text", "timestamp");

  List<Page> pages;
  Page currentPage = null;
  private StringBuilder data = null;

  protected final String outputDirectory;
  private final int batchSize;

  private final int maxTextSize;

  private int flushCount = 0;

  private Stack<String> location = new Stack<>();

  public int offset = 0;

  public WikipediaHandler(String outputDirectory, int batchSize, int maxTextSize) {
    this.outputDirectory = outputDirectory;
    this.batchSize = batchSize;
    this.maxTextSize = maxTextSize;
    pages = new ArrayList<>(batchSize);
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) {
    if (qName.equalsIgnoreCase("page")) {
      Page p = new Page();
      pages.add(p);
      currentPage = p;
    }
    if (interestingElements.contains(qName.toLowerCase())) {
      data = new StringBuilder();
    }
    location.add(qName);
  }

  public void endElement(String uri, String localName, String qName) {
    if (!qName.equals(location.pop())) {
      throw new RuntimeException("Something is wrong in the stack");
    }

    if (qName.equalsIgnoreCase("page")) {
      currentPage = null;
      if (pages.size() == batchSize) {
        if (Integer.parseInt(pages.get(0).id) < offset) {
          System.out.println("Skipping " + pages.size() + " docs, starting at " + pages.get(0).id);
        } else {
          flush();
        }
        flushCount++;
        pages.clear();
      }
    } else if (qName.equalsIgnoreCase("title")) {
      if (currentPage != null) {
        currentPage.title = data.toString();
        data = null;
      }
    } else if (qName.equalsIgnoreCase("id") && "page".contentEquals(location.peek())) {
      if (currentPage != null) {
        currentPage.id = data.toString();
        data = null;
        if (Integer.parseInt(currentPage.id) < offset) {
          currentPage = null;
        }
      }
    } else if (qName.equalsIgnoreCase("username")) {
      if (currentPage != null) {
        currentPage.contributor = data.toString();
        data = null;
      }
    } else if (qName.equalsIgnoreCase("timestamp")) {
      if (currentPage != null) {
        currentPage.date = data.toString();
        data = null;
      }
    } else if (qName.equalsIgnoreCase("text")) {
      if (currentPage != null) {
        if (data.length() > maxTextSize) {
          int truncation = maxTextSize;

          // Make sure we're not splitting surrogate characters
          if (Character.isHighSurrogate(data.charAt(truncation - 1))) {
            truncation--;
          }

          data.setLength(truncation);
        }

        currentPage.text = data.toString();
        data = null;
      }
    } else if (qName.equalsIgnoreCase("comment")) {
      if (currentPage != null) {
        currentPage.comment = data.toString();
        data = null;
      }
    }

  }

  @Override
  public void endDocument() {
    flush();
  }

  abstract void flush();

  final int getFlushCount() {
    return flushCount;
  }

  @Override
  public void characters(char[] ch, int start, int length) {
    if (data != null) {
      data.append(new String(ch, start, length));
    }
  }

}
