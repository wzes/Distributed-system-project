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
#### Streaming 中不能使用外部的 rdd

- 通过 Broadcast 引入的 rdd

  ```
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

  ​

- 在内部引入spark session 解决问题

  ```
  INPUT.foreachRDD(rdd => {
        val strings: Array[String] = rdd.collect()
        if (strings.length > 0) {
          val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
          val df = spark.read.parquet(filename.toString())
          val AUTHOR = strings(0).substring(strings(0).indexOf("author:") + 7).trim

          val df1 = df.filter(row =>
            if (row(1) != null) {
              row(1).toString.contains(AUTHOR)
            } else false
          ).orderBy(-df("year")).select("title")
          df1.cache()
          println(df1.count())
          df1.collect().foreach(line => {
            println(line(0))
          })
          //df1.foreach(println(_))
        }
      })
  ```

#### 内部引入每次都需要重新导入数据

- 使用全局变量，只加载一次

  ```
  	// var df = null
      var df:DataFrame = null

      INPUT.foreachRDD(rdd => {
        val strings: Array[String] = rdd.collect()
        if (strings.length > 0) {
          val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
          // cached input data
          if (df == null) {
            df = spark.read.parquet(filename.toString)
            df.cache()
          }
          //val df = spark.read.parquet(filename.toString())
          val AUTHOR = strings(0).substring(strings(0).indexOf("author:") + 7).trim

          val df1 = df.filter(row =>
            if (row(1) != null) {
              row(1).toString.contains(AUTHOR)
            } else false
          ).orderBy(-df("year")).select("title")
          df1.cache()
          println(df1.count())
          df1.collect().foreach(line => {
            println(line(0))
          })
          //df1.foreach(println(_))
        }
      })
  ```

### 性能优化

- 数据整理

  ```
  将原始数据进行处理，将所需要的列提取并存为parquet格式，经过处理的数据只剩下300M左右，大大提高了效率。
  ```

  ​

- 参数调优

  ```
  --driver-memory 2G
  --num-executors 10
  --executor-memory 2G
  --executor-cores 5
  ```

- 结果缓存

  ```
  首次计算需要导入数据 在 10s 左右能得到结果；
  之后通过缓存 dataframe 使计算时间在 1s 左右完成；
  ```

- parquet 文件块数量 

  ```
  数量与 cpu 核数接近则会加快速度
  ```

### 注意事项
- 答案与现场搜的答案不唯一，网站上的结果与本机的结果可能存在差异，网站将标题中中出现的人名也计算在相关论文内，实际上根据作者是搜索不到的。

- 运行命令

  ```
  spark-submit --master spark://cluster01:7077 --num-executors 10 --driver-memory 2G --executor-memory 2G --class com.distributed.application.hw4.HW4 DC-HW4.jar 1 hdfs://cluster01:8020/dblp/dblp-hw4.parquet 59.110.136.134 10001
  ```

  ​

### 附件

- HW4.scala 为三个部分合一的代码, DataHelper.scala 为数据预处理的代码，其余为各个部分的代码。
- 答辩ppt
- 程序运行jar包

