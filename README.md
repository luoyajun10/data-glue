# Data Tunnel

DataTunnel 简称数据通道，是一个高效的数据同步组件。支持使用Spark引擎将Kafka中的CDC数据同步到HBase/Kudu/ES/MySQL等存储端，实现实时入仓或入湖。

## 构建

DataTunnel 使用Maven编译打包，可运行如下命令：

```shell
mvn -DskipTests clean package
```

## 使用限制

DataTunnel 功能还在不断完善中，目前有如下使用方面的限制：

- 多引擎多数据源支持。目前仅支持使用Spark引擎同步Kafka中的CDC数据，因此对于使用其他主流引擎如Flink的场景不支持。Kafka的数据格式有一定要求。
- 数据处理方式。目前仅支持以流处理的方式实时入仓或入湖，不支持批量入仓或入湖。由于该项目是从生产环境实时同步链路中孵化出来的，暂未考虑批处理的实现方式（后续可考虑提供通用的工具类）。
- 数据转换逻辑。目前仅支持过滤表操作，不支持复杂的数据转换。后续考虑抽象出一层 Transformer 转换层。
