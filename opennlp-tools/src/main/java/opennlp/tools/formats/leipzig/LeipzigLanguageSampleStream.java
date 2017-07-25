/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package opennlp.tools.formats.leipzig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageSample;
import opennlp.tools.util.MarkableFileInputStreamFactory;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;

public class LeipzigLanguageSampleStream implements ObjectStream<LanguageSample> {

  private class LeipzigSentencesStream implements ObjectStream<LanguageSample> {
    private final String lang;
    private int sentencesPerSample;
    private int numberOfSamples;

    private ObjectStream<String> lineStream;
    private int sampleCount;

    LeipzigSentencesStream(String lang, File sentencesFile, int sentencesPerSample, int numberOfSamples)
        throws IOException {
      this.lang = sentencesFile.getName().substring(0, 3);
      this.sentencesPerSample = sentencesPerSample;
      this.numberOfSamples = numberOfSamples;

      lineStream = new PlainTextByLineStream(new MarkableFileInputStreamFactory(sentencesFile),
          StandardCharsets.UTF_8);
    }

    @Override
    public LanguageSample read() throws IOException {

      if (sampleCount < numberOfSamples) {
        StringBuilder sampleString = new StringBuilder();

        int count = 0;
        String line;
        while (count < sentencesPerSample && (line = lineStream.read()) != null) {

          int textStart = line.indexOf('\t') + 1;

          // TODO: It should it be changed to contain an array of sample strings ?!
          sampleString.append(line.substring(textStart) + " ");

          count++;
        }

        if (sampleString.length() > 0) {
          sampleCount++;
          return new LanguageSample(new Language(lang), sampleString);
        }
      }
      return null;
    }
  }

  private final int sentencesPerSample;

  private Map<String, Integer> langSampleCounts;
  private File[] sentencesFiles;
  private Map<String, LinkedHashSet<Long>> selectedRandomLines = new HashMap<>();

  private Iterator<File> sentencesFilesIt;
  private ObjectStream<LanguageSample> sampleStream;

  private Random random;

  public LeipzigLanguageSampleStream(File leipzigFolder, final int sentencesPerSample,
                                     final int samplesPerLanguage) throws IOException {
    this.sentencesPerSample = sentencesPerSample;
    // TODO: Use a FileFilter to make this more reliable in case there are files which should be ignored
    sentencesFiles = leipzigFolder.listFiles();
    Arrays.sort(sentencesFiles);

    long sentencesPerLanguage = sentencesPerSample * samplesPerLanguage;

    // lang -> filename -> entries
    Map<String, Map<File, List<Integer>>> langEntriesMap = new HashMap<>();
    for (File file: sentencesFiles) {
      String lang = file.getName().substring(0, 3);
      if (!langEntriesMap.containsKey(lang)) {
        langEntriesMap.put(lang, new HashMap<>());
      }

      langEntriesMap.get(lang).put(file, new ArrayList<>());
    }

    for(Map.Entry<String, Map<File, List<Integer>>> entry: langEntriesMap.entrySet()) {
      String lang = entry.getKey();
      System.out.println("Getting lines for lang: " + lang);
      Set<File> files = entry.getValue().keySet();

      Map<File, Long> sizeMap = new HashMap<>();
      long totalAvailableSize = 0;
      for (File file: files) {
        long size = scanFileToGetSize(file);
        System.out.println(file + " " + size);
        totalAvailableSize += size;
        sizeMap.put(file, size);
      }
      long sentencesToCollect = Math.min(sentencesPerLanguage, totalAvailableSize);
      if (sentencesToCollect < sentencesPerLanguage) {
        System.err.println("warning: not enough senteces for language: " + lang + ". Using total" +
            " using total available.");
      }

      // now we can compute the number of lines we get from each file
      // we can do it weighted by the file size

      // TODO







    }

    Map<String, Integer> langCounts = Arrays.stream(sentencesFiles)
        .map(file -> file.getName().substring(0, 3))
        .collect(Collectors.groupingBy(String::toString, Collectors.summingInt(v -> 1)));

    langSampleCounts = langCounts.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> samplesPerLanguage / e.getValue()));

    random = new Random();

    long startShuffling = System.currentTimeMillis();

    for (File sentenceFile: sentencesFiles) {
      String lang = sentenceFile.getName().substring(0, 3);
      int samples = langSampleCounts.get(lang);
      System.out.print("Shuffling " + sentenceFile.getName() + " ... ");
      long start = System.currentTimeMillis();
      List<Long> lineIndex = createLineIndex(sentenceFile);


      LinkedHashSet<Long> selectedLines = new LinkedHashSet<>(samples);

      PrimitiveIterator.OfInt randomGenerator = random
          .ints(0, lineIndex.size())
          .iterator();
      while (selectedLines.size() < samples) {
        selectedLines.add(lineIndex.get(randomGenerator.next()));
      }

      selectedRandomLines.put(sentenceFile.getName(), selectedLines);
      long time = System.currentTimeMillis() - start;
      System.out.println("finished in (ms) " + time);
    }

    System.out.println("Total shuffling time in (ms) " + (System.currentTimeMillis() - startShuffling));
    reset();
  }

  /**
   * Gets the file size by counting new lines
   * @param file
   * @return the number of lines
   * @throws IOException
   */
  private long scanFileToGetSize(File file) throws IOException {
    return Files.lines(file.toPath()).count();
  }

  public LanguageSample read() throws IOException {
    LanguageSample sample;
    if (sampleStream != null && (sample = sampleStream.read()) != null) {
      return sample;
    }
    else {
      if (sentencesFilesIt.hasNext()) {
        File sentencesFile = sentencesFilesIt.next();
        System.out.println(sentencesFile);
        String lang = sentencesFile.getName().substring(0, 3);

        sampleStream = new LeipzigSentencesStream(lang, sentencesFile,
            sentencesPerSample, langSampleCounts.get(lang));

        return read();
      }
    }
    return null;
  }

  @Override
  public void reset() throws IOException {
    sentencesFilesIt = Arrays.asList(sentencesFiles).iterator();
    sampleStream = null;
  }

  /**
  * Gets the position of lines so we can scan the file using RandomAccessFile
   */
  private List<Long> createLineIndex(File file) throws IOException {
    List<Long> lineIndex = new ArrayList<>();
    long byteCount = 0;
    int lineCount = 1;
    final int lineEndingLength = "\n".getBytes(StandardCharsets.UTF_8).length;
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = br.readLine()) != null) {
        lineIndex.add(byteCount);
        if(!line.startsWith(Integer.toString(lineCount))) {
          // skip
        }
        byteCount += line.getBytes(StandardCharsets.UTF_8).length + lineEndingLength;
        lineCount++;
      }
    }
    return lineIndex;
  }

  public static void main(String[] args) throws Exception {
    new LeipzigLanguageSampleStream(new File("/Volumes/Seagate Expansion Drive/opt/leipzig/leipzig-train/"),
        5, 2000);
  }
}
