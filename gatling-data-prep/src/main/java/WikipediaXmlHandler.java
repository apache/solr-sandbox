import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileOutputStream;

public class WikipediaXmlHandler extends WikipediaHandler {
    public WikipediaXmlHandler(String outputDirectory, int batchSize, int maxTextSize) {
        super(outputDirectory, batchSize, maxTextSize);
    }

    @Override
    void flush() {
        Document doc;
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try {
            // use factory to get an instance of document builder
            DocumentBuilder db = dbf.newDocumentBuilder();
            // create instance of DOM
            doc = db.newDocument();

            // create the root element
            Element rootElement = doc.createElement("add");
            doc.appendChild(rootElement);

            for (Page p:pages) {
                Element pageDoc = doc.createElement("doc");
                rootElement.appendChild(pageDoc);
                String[][] fields = p.toFields();
                for (int i = 0; i < fields.length; i++) {
                    Element fieldElement = doc.createElement("field");
                    pageDoc.appendChild(fieldElement);
                    fieldElement.setAttribute("name", fields[i][0]);
                    fieldElement.setTextContent(fields[i][1]);
                }
            }

            try {
                Transformer tr = TransformerFactory.newInstance().newTransformer();
                tr.setOutputProperty(OutputKeys.INDENT, "yes");
                tr.setOutputProperty(OutputKeys.METHOD, "xml");
                tr.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
                tr.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
                tr.transform(new DOMSource(doc),
                        new StreamResult(new FileOutputStream(new File(outputDirectory, "output" + getFlushCount() + ".xml"))));
                System.out.println("Flushed output" + getFlushCount() + ".xml, containing " + pages.size() + " docs, starting at " + pages.get(0).id);
            } catch (Exception e) {
                throw new RuntimeException("Failed to flush on document starting with " + pages.get(0), e);
            }
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

}
