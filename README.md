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
### 遇到的坑
- TODO
```
外部数据太大，使用广播变量速度过慢
```
- Streaming 中不能使用外部的通过 Broadcast 引入的 Rdd
```$xslt
使用 scala 数据结构解决 使用 df.collect() 将 DataFrame 收集到Broadcast中，
val rows: Array[Row] = df.collect()
在 Input 中使用
INPUT.foreachRDD(rdd =>
      rdd.foreach { line => {
        val AUTHOR = line.substring(line.indexOf("author:") + 7).trim
        val rows: Array[Row] = broadcastDF.value.filter(row => {
          if (row(1) != null) {
            row(1).toString.contains(AUTHOR)
          } else false
        })
        println(rows.length)
        val tuples: Array[(String, Int)] = rows.map(row => (row(0).toString, Integer.parseInt(row(2).toString)))
        tuples.sortWith(_._2 > _._2).foreach(line => {
          println(line._1 + " : " + line._2)
        })
      }
    })
```