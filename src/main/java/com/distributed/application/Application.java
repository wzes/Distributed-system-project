package com.distributed.application;

import org.apache.commons.lang3.StringEscapeUtils;

import java.io.*;

public class Application {

    // You must be do with the xml data
    public static void main(String[] args) {
        File inFile = new File("/d1/documents/DistributeCompute/dblp.xml");
        File outFile = new File("/d1/documents/DistributeCompute/dblp-out.xml");

        try {
            FileReader fileReader = new FileReader(inFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            outFile.createNewFile();
            FileWriter fileWriter = new FileWriter(outFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            String str;
            long index = 0;
            long tag = index;
            while ((str = bufferedReader.readLine()) != null) {
                String line = str.replace("inproceedings", "article")
                        .replace("<www", "<article")
                        .replace("</www>", "</article>");
                bufferedWriter.write(StringEscapeUtils.unescapeHtml4(line) + "\n");
                long poc = index * 100 / 54139538;
                if (poc != tag) {
                    tag = poc;
                    System.out.println("process: " + poc + "%");
                }
                index++;
            }

            bufferedWriter.flush();
            fileReader.close();
            fileWriter.close();
            bufferedReader.close();
            bufferedWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
