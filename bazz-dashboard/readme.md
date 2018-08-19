## dashboard-generate模块
#### 功能
1. 用来模拟生成用户数据和订单数据
2. 生成数据发送到kafka `topic-dashboard-generate-order`,`topic-dashboard-generate-user-register`
3. 数据的生成会从redis获取 `dashboard-generate-user-max-id`
4. 定时更新redis中的`dashboard-generate-user-max-id`
5. handler.sh 为发布的启动脚本

#### 打包
打包的本地路径需在pom中设置

## dashboard-storm模块
kafka发出数据，通过Storm处理数据，将数据持久化到mysql。

## dashboard-web模块
从mysql中获取数据,web进行展示如:日活跃用户数,订单支付次数,注册用户城市分布状况




