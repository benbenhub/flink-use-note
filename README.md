# flink-use-note

<p align="left">
  <img src="https://img.shields.io/github/stars/benbenhub/flink-use-note?style=social" alt="npm version" />
</p>

## 介绍

Flink Java 使用笔记，Flink源码解析，数据处理逻辑记录，Flink Connectors部分参考[纯钧](https://github.com/DTStack/chunjun)
，将不同的数据库链接封装在core，cases列举Flink算子使用样例和流式数据处理逻辑样例。

## 参数配置



## source

在Flink-v1.18之前的版本有两套API，DataStream API（有界或无界流数据）以及 DataSet API（有界数据集）,
分别讨论这两种API source的定义方式。

### DataSet
用`ExecutionEnvironment.createInput()`创建source，重写抽象类`RichInputFormat`连接不同的数据库。
比如腾讯云的COS[`CosOutputFormat`](flinkuse-core/src/main/java/com/flinkuse/core/connector/cos/CosOutputFormat.java)

### DataStream
`StreamExecutionEnvironment`创建source有三种方式，`StreamExecutionEnvironment.createInput()`、`StreamExecutionEnvironment.addSource()`
以及`StreamExecutionEnvironment.fromSource()`。
- `createInput()`方法需要重写抽象类`RichInputFormat`，但是只支持无界数据流不可以将RuntimeExecutionMode设置为BATCH。
- `addSource()`方法需要实现`SourceFunction`类，同样addSource也只支持无界数据流不可以将RuntimeExecutionMode设置为BATCH。
- `fromSource()`方法需要实现`Source`类

### 总结
重写抽象类`RichInputFormat`的连接方式可以分别应用在DataStream API以及 DataSet API，应用在DataStream API时要想自定义运行模式除了使用
fromSource方法还可以用以下代码的方式自定义`Boundedness`（有界流/无界流）：
```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
InputFormatSourceFunction<String> function =
                new InputFormatSourceFunction<>(new CosInputFormat(cosObjs), BasicTypeInfo.STRING_TYPE_INFO);

DataStream<String> s = new DataStreamSource<>(env,
        BasicTypeInfo.STRING_TYPE_INFO,
        new StreamSource<>(function),
        false,
        "COS Source",
        Boundedness.BOUNDED);
        
s.print();
```
在Flink-v1.18版本中InputFormatSourceFunction类已经带有删除线，自定义的source最好使用fromSource方法实现Source类。

## sink

## AsyncIO

## 批流一体
