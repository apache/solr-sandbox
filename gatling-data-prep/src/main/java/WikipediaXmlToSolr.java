import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;

public class WikipediaXmlToSolr {

  public static void main(String[] args) throws IOException {
    if (args.length < 5 || args.length > 6) {
      System.out.println("Expecting five positional arguments: source-file, output-dir, output-data-type, batchSize, maxTextSize, [docOffset]");
      System.exit(1);
    }

    final var sourceFilePath = args[0];
    final var outputDirPath = args[1];
    final var outputDataType = args[2];
    final var batchSize = Integer.parseInt(args[3]);
    final var maxTextSize = Integer.parseInt(args[4]);

    WikipediaHandler handler = null;
    if ("json".equals(outputDataType)) {
      handler = new WikipediaJsonHandler(outputDirPath, batchSize, maxTextSize);
    } else if ("xml".equals(outputDataType)) {
      handler = new WikipediaXmlHandler(outputDirPath, batchSize, maxTextSize);
    } else {
      System.out.println("data-type should be 'xml' or 'json'");
      System.exit(1);
    }
    if (args.length > 5) {
      handler.offset = Integer.parseInt(args[5]);
    }
    try {
      File f = new File(sourceFilePath);
      SAXParserFactory factory = SAXParserFactory.newInstance();
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, false);
      SAXParser parser = factory.newSAXParser();
      parser.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "false");
      parser.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "false");
      parser.parse(f, handler);
    } catch (ParserConfigurationException | SAXException e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    }
  }
}
