# lealone-olap
可暂停的渐进式 OLAP 引擎

## 启用 OLAP 引擎

`set olap_threshold 1000;`

当执行 select 语句时，如果遍历了1000条记录还没有结束就会自动启用 OLAP 引擎
