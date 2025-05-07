# spark-log-analysis-visualization

## 用法

1. 编译打包 spark-realtime 里面的两个项目(用idea)

2. 运行 run.sh

    第一次运行请取消运行 download-es-analysis-ik.sh 注释

3. 运行模拟程序

    `docker compose run --rm collection [app|db] [mock.date]`

    app: 生成日志数据
    db: 生成业务数据

4. 访问网站

    - [电商统计管理平台](http://bigdata.gmall.com/)

    - [kibana](http://localhost:5601)

        你可以根据存入es的数据创建想要的图表
