package org.apache.iotdb.db.utils.datastructure;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.List;
import java.util.stream.Stream;

public class CsvReader {
  public static int readCSV(String filePath, int threshold, long[] timestamps)
          throws IOException {
    CSVParser csvRecords =
            CSVFormat.Builder.create(CSVFormat.DEFAULT)
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setQuote('`')
                    .setEscape('\\')
                    .setIgnoreEmptyLines(true)
                    .build()
                    .parse(new InputStreamReader(Files.newInputStream(Paths.get(filePath))));

    Stream<CSVRecord> records = csvRecords.stream();
    Object[] objects = records.toArray();

    int timeColumnIndex = 0;
    int cnt = 0;
    for (Object o : objects) {
      CSVRecord object = (CSVRecord) o;
      List<String> vals = object.toList();
      long timestamp = Long.parseLong(vals.get(timeColumnIndex));
      timestamps[cnt] = timestamp;
      cnt++;
      if (cnt >= threshold) break;
    }
    System.gc();
    return cnt;
  }
}
