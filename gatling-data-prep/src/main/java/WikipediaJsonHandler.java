import com.google.gson.stream.JsonWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

public class WikipediaJsonHandler extends WikipediaHandler {
    public WikipediaJsonHandler(String outputDirectory, int batchSize, int maxTextSize) {
        super(outputDirectory, batchSize, maxTextSize);
    }

    @Override
    void flush() {
        try (JsonWriter writer = new JsonWriter(new FileWriter(new File(outputDirectory, "output" + getFlushCount() + ".json")))) {
            writer.beginArray();
            for (Page p : pages) {
                writer.beginObject();
                for (String[] pageField : p.toFields()) {
                    writer.name(pageField[0]).value(pageField[1]);
                }
                writer.endObject();
            }
            writer.endArray();
            System.out.println("Flushed output" + getFlushCount() + ".json, containing " + pages.size() + " docs, starting at " + pages.get(0).id);
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush on document starting with " + pages.get(0), e);
        }
    }
}


