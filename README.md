# lealone-olap

可暂停的渐进式 OLAP 引擎


## 编译需要

* jdk 17+
* maven 3.8+


## 打包插件

运行 `mvn clean package -Dmaven.test.skip=true`

生成 jar 包 `target\lealone-olap-plugin-6.0.0.jar`

假设 jar 包的绝对路径是 `E:\lealone\lealone-plugins\olap\target\lealone-olap-plugin-6.0.0.jar`


## 运行插件

先参考[ lealone 快速入门](https://github.com/lealone/Lealone-Docs/blob/master/应用文档/Lealone数据库快速入门.md) 启动 lealone 数据库并打开一个命令行客户端

然后执行以下命令创建插件：

```sql
create plugin olap
  implement by 'com.lealone.plugins.olap.OlapPlugin' 
  class path 'E:\lealone\lealone-plugins\olap\target\lealone-olap-plugin-6.0.0.jar';
```

要 drop 插件可以执行以下命令：

```sql
drop plugin olap;
```

执行 drop plugin 会把插件占用的内存资源都释放掉


## 启用 OLAP 引擎

`set olap_threshold 1000;`

当执行 select 语句时，如果遍历了1000条记录还没有结束就会自动启用 OLAP 引擎


