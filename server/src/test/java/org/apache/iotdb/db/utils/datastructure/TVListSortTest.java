package org.apache.iotdb.db.utils.datastructure;

import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TVListSortTest {
  final String root = "D:\\Code\\iotdb\\memtable\\";

  @Test
  public void testOrdered() {
    System.out.print("Test totally ordered:\n");
    String filePath = root + "synthetic\\log_normal\\n_6_mu_0_sigma_0.csv";
    testSortAlgorithm(filePath);
  }

  @Test
  public void testSimulateQuery() throws IOException {
    String[] filePaths = new String[]{
            root + "datasets\\citibike\\201808.csv",
            root + "datasets\\citibike\\201809.csv" ,
            root + "datasets\\citibike\\201810.csv" ,
            root + "datasets\\citibike\\201811.csv" ,
            root + "datasets\\citibike\\201812.csv" ,
            root + "datasets\\citibike\\201901.csv" ,
            root + "datasets\\citibike\\201902.csv"
    };
    simulateBenchmarkQuery(filePaths);
  }


  @Test
  public void testArraySize(){
    String[] filePaths = new String[]{
            root + "datasets\\citibike\\201809.csv",
            root + "datasets\\samsung\\s-10_1e6.csv",
            root + "synthetic\\lognormal\\n_6_mu_2_sigma_4.csv",
            root + "synthetic\\lognormal\\n_6_mu_0_sigma_1.csv",
            root + "synthetic\\absnormal\\n_6_mu_2_sigma_4.csv",
            root + "synthetic\\absnormal\\n_6_mu_0_sigma_1.csv"
    };
    for(String filePath : filePaths){
      testSortAlgorithm(filePath, 10000);
      testSortAlgorithm(filePath, 100000);
      testSortAlgorithm(filePath, 1000000);
    }
  }

  @Test
  public void testSynthetic() {
    System.out.print("Test LogNormal ordered:\n");
    String[] mus = new String[]{"0", "0.25", "0.5", "1", "2", "4", "8"};
    String[] sigmas = new String[]{"0.25", "0.5", "1", "2", "4", "8"};

    String logNormal = root + "synthetic\\lognormal\\n_6_mu_%s_sigma_%s.csv";
    String absNormal = root + "synthetic\\absnormal\\n_6_mu_%s_sigma_%s.csv";
    for (int j = 0; j <= 5; j++) {
      testSortAlgorithm(String.format(logNormal, "1", sigmas[j]));
      testSortAlgorithm(String.format(absNormal, "1", sigmas[j]));
    }
    for (int i = 0; i <= 6; i++) {
      testSortAlgorithm(String.format(logNormal, mus[i], "1"));
      testSortAlgorithm(String.format(absNormal, mus[i], "1"));
    }
    for (int j = 0; j <= 5; j++) {
      testSortAlgorithm(String.format(logNormal, "4", sigmas[j]));
      testSortAlgorithm(String.format(absNormal, "4", sigmas[j]));
    }
    for (int i = 0; i <= 6; i++) {
      testSortAlgorithm(String.format(logNormal, mus[i], "4"));
      testSortAlgorithm(String.format(absNormal, mus[i], "4"));
    }
  }

  @Test
  public void testRealWorld() {
    String filePath;
    filePath = root + "datasets\\samsung\\d-5_1e6.csv";
    testSortAlgorithm(filePath);
    filePath = root + "datasets\\samsung\\s-10_1e6.csv";
    testSortAlgorithm(filePath);
    filePath = root + "datasets\\citibike\\201808.csv";
    testSortAlgorithm(filePath);
    filePath = root + "datasets\\citibike\\201902.csv";
    testSortAlgorithm(filePath);
  }

  @Test
  public void testVaryingBlockSize(){
    String[] mus = new String[]{"0", "0.25", "0.5", "1", "2", "4", "8"};
    String[] sigmas = new String[]{"0.25", "0.5", "1", "2", "4", "8"};

    String logNormal = root + "synthetic\\lognormal\\n_6_mu_%s_sigma_%s.csv";
    String absNormal = root + "synthetic\\absnormal\\ n_6_mu_%s_sigma_%s.csv";
    testBackwardBlockSize(String.format(logNormal, 8, 4));
    for(int i = 0; i <= 6; i++){
      for(int j = 0; j <= 5; j++){
        String filePath = String.format(logNormal, mus[i], sigmas[j]);
        testBackwardBlockSize(filePath);
      }
    }
    for(int i = 0; i <= 6; i++){
      for(int j = 0; j <= 5; j++){
        String filePath = String.format(absNormal, mus[i], sigmas[j]);
        testBackwardBlockSize(filePath);
      }
    }
    String filePath = root + "datasets\\samsung\\d-5_1e6.csv";
    testBackwardBlockSize(filePath);
    filePath = root + "datasets\\samsung\\s-10_1e6.csv";
    testBackwardBlockSize(filePath);
    filePath = root + "datasets\\citibike\\201808.csv";
    testBackwardBlockSize(filePath);
    filePath = root + "datasets\\citibike\\201902.csv";
    testBackwardBlockSize(filePath);
  }


  public void testBackwardBlockSize(String filePath){
    int N = 1000000;
    long[] timestamps = new long[N];
    HashMap<String, List> data = new HashMap<>();
    try {
      CsvReader.readCSV(filePath, N, timestamps);
    } catch (IOException e) {
      System.out.print("parse false");
    }
//    System.out.printf(
//        "%s, length=%d, inversions=%d, rates=%.2f\n", filePath, N, (int) inversions, inversions / N);
  BackIntTVList preTVList = new BackIntTVList();

    for (int k = 0; k < 1; k++) {
      for (int i = 0; i < N; i++) {
        long t = timestamps[i];
        preTVList.putInt(t, i);
      }
    }
    double begin = System.currentTimeMillis();
    preTVList.sort();
    double end = System.currentTimeMillis();
//    System.out.printf("%.2f\n", end - begin);
    int L = 1, maxL= 1000000;
    if(end - begin > 100){
      L = 16;
    }
    if(end - begin > 200){
      L = 16 * 16;
    }
    if(end - begin > 300){
      L = 16 * 16 * 16;
    }
    while(L <= maxL) {
      BackIntTVList backwardIntTVList = new BackIntTVList();
      for (int k = 0; k < 1; k++) {
        for (int i = 0; i < N; i++) {
          long t = timestamps[i];
          backwardIntTVList.putInt(t, i);
        }
      }
      begin = System.currentTimeMillis();
      backwardIntTVList.sort(L);
      end = System.currentTimeMillis();
      System.out.printf("%s, %d, %.2f\n", filePath, L, end - begin);
      preTVList.clear();
      backwardIntTVList.clear();
      L *= 2;
    }
  }

  public void testSortAlgorithm(String filePath) {
    // System.out.printf("============================filePath=%s======================\n", filePath);
    int N = 1000000;
    long[] timestamps = new long[N];
    try {
      N = CsvReader.readCSV(filePath, N, timestamps);
    } catch (IOException e) {
      System.out.print("parse false");
    }

    BackIntTVList preTVList = new BackIntTVList();
    BackIntTVList backwardIntTVList = new BackIntTVList();
    TimIntTVList timIntTVList = new TimIntTVList();
    QuickIntTVList quickIntTVList = new QuickIntTVList();
    // PatienceIntTVList patienceIntTVList = new PatienceIntTVList();
    for (int k = 0; k < 1; k++) {
      for (int i = 0; i < N; i++) {
        long t = timestamps[i];
        int v = i;
        preTVList.putInt(t, v);
        backwardIntTVList.putInt(t, v);
        timIntTVList.putInt(t, v);
        quickIntTVList.putInt(t, v);
        // patienceIntTVList.putInt(t, v);
      }
    }
    double begin, end;
    // preTVList.sort();
    begin = System.currentTimeMillis();
    backwardIntTVList.sort();
    end = System.currentTimeMillis();
    System.out.printf("%s, Backward, %.2f\n", filePath, end - begin);

    begin = System.currentTimeMillis();
    quickIntTVList.sort();
    end = System.currentTimeMillis();
    System.out.printf("%s,Quick, %.2f\n",filePath, end - begin);

    begin = System.currentTimeMillis();
    timIntTVList.sort();
    end = System.currentTimeMillis();
    System.out.printf("%s,Tim, %.2f\n",filePath, end - begin);

    begin = System.currentTimeMillis();
    // patienceIntTVList.sort();
    end = System.currentTimeMillis();
    System.out.printf("%s,Patience, %.2f\n",filePath, end - begin);

    for (int i = backwardIntTVList.rowCount - 1; i >= 0; i--) {
      if (backwardIntTVList.getTime(i) != timIntTVList.getTime(i)) {
        System.out.printf(
            "1 i=%d %d %d\n ", i, backwardIntTVList.getTime(i), timIntTVList.getTime(i));
        break;
      }
      if (quickIntTVList.getTime(i) != timIntTVList.getTime(i)) {
        System.out.printf("2 i=%d\n", i);
      }
    }
    preTVList.clear();
    backwardIntTVList.clear();
    quickIntTVList.clear();
    timIntTVList.clear();
  }

  public void testSortAlgorithm(String filePath, int size) {
//    System.out.printf("============================filePath=%s======================\n", filePath);
    int N = size;
    long[] timestamps = new long[N];
    try {
      CsvReader.readCSV(filePath, N, timestamps);
    } catch (IOException e) {
      System.out.print("parse false");
    }
    BackIntTVList preTVList = new BackIntTVList();
    BackIntTVList backwardIntTVList = new BackIntTVList();
    TimIntTVList timIntTVList = new TimIntTVList();
    QuickIntTVList quickIntTVList = new QuickIntTVList();
    // PatienceIntTVList patienceIntTVList = new PatienceIntTVList();
    for (int k = 0; k < 1; k++) {
      for (int i = 0; i < N; i++) {
        long t = timestamps[i];
//        preTVList.putInt(t, i);
        backwardIntTVList.putInt(t, i);
        timIntTVList.putInt(t, i);
        quickIntTVList.putInt(t, i);
        // patienceIntTVList.putInt(t, i);
      }
    }
    double begin, end;
    // preTVList.sort();
    begin = System.currentTimeMillis();
    backwardIntTVList.sort();
    end = System.currentTimeMillis();
    System.out.printf("%s, %d, Backward, %.2f\n", filePath, size, end - begin);
//
    begin = System.currentTimeMillis();
    quickIntTVList.sort();
    end = System.currentTimeMillis();
    System.out.printf("%s, %d, Quick, %.2f\n",filePath, size, end - begin);

    begin = System.currentTimeMillis();
    timIntTVList.sort();
    end = System.currentTimeMillis();
    System.out.printf("%s, %d, Tim, %.2f\n",filePath, size, end - begin);

    begin = System.currentTimeMillis();
    // patienceIntTVList.sort();
    end = System.currentTimeMillis();
    System.out.printf("%s, %d, Patience, %.2f\n",filePath, size, end - begin);

    preTVList.clear();
    backwardIntTVList.clear();
    quickIntTVList.clear();
    timIntTVList.clear();
    // patienceIntTVList.clear();
  }

  public void simulateBenchmarkQuery(String[] filePaths) throws IOException {
//    System.out.printf("============================filePath=%s======================\n", filePath);
    long[] timestamps = new long[2000000];

    int length = 0;
    int N = 9000000;
    int fileIndex = 0;
    int k = 0;
    BackIntTVList preTVList = new BackIntTVList();
    BackIntTVList backwardIntTVList = new BackIntTVList();
    TimIntTVList timIntTVList = new TimIntTVList();
    QuickIntTVList quickIntTVList = new QuickIntTVList();
    for (int i = 0; i < 8000000; i++, k++) {
      if(i >= length) {
        length += CsvReader.readCSV(filePaths[fileIndex], 2000000, timestamps);
        fileIndex += 1;
        k = 0;
      }
      long t = timestamps[k];
      backwardIntTVList.putInt(t, k);
      timIntTVList.putInt(t, k);
      quickIntTVList.putInt(t, k);
      if(i % 10000 == 0){
        double begin, end;
        // preTVList.sort();
        begin = System.currentTimeMillis();
        backwardIntTVList.sort();
        end = System.currentTimeMillis();
        System.out.printf("%s, %d, Backward, %.2f\n", filePaths[fileIndex], i, end - begin);

        begin = System.currentTimeMillis();
        quickIntTVList.sort();
        end = System.currentTimeMillis();
        System.out.printf("%s, %d, Quick, %.2f\n",filePaths[fileIndex], i, end - begin);

        begin = System.currentTimeMillis();
        timIntTVList.sort();
        end = System.currentTimeMillis();
        System.out.printf("%s, %d, Tim, %.2f\n",filePaths[fileIndex], i, end - begin);
      }
      if(i % 100000 == 0){
        backwardIntTVList.clear();
        quickIntTVList.clear();
        timIntTVList.clear();
        backwardIntTVList = new BackIntTVList();
        timIntTVList = new TimIntTVList();
        quickIntTVList = new QuickIntTVList();
      }

    }

  }

}
