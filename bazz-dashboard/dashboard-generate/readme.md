### dashboard-generate项目
#### 功能
1. 用来模拟生成用户数据和订单数据
2. 生成数据发送到kafka `topic-dashboard-generate-order`,`topic-dashboard-generate-user-register`
3. 数据的生成会从redis获取 `dashboard-generate-user-max-id`
4. 定时更新redis中的`dashboard-generate-user-max-id`(TODO)
