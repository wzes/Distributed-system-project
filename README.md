# Distributed-system-project
Tuesday, 26. December 2017 11:41PM 
### xml 数据清洗
- 该文件存在HTML转义字符，在使用之前先进行转化
- xml文件中有些节点不是article类型的，又出现www inproceedings 需要对其进行替换，否则解析不到
```
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
```