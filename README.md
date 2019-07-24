# lua-rabbitmq 接口说明


---
**工厂函数，如引入库名为 rabbitmq**
* 新建一个MQ对象（连接对象） <br/>
参数：无<br/>
返回值：mq对象（连接对象）<br/>
**rabbitmq.newRabbitmq()**
<br/><br/>


**mq对象成员方法，如对象名为 mq**
* 配置mq连接参数 <br/>
参数1(string)：host, 主机名<br/>
参数2(number)：port, 端口<br/>
参数3(string)：username, 登录mq的用户名<br/>
参数4(string)：password, 用户密码<br/>
参数5(string, opt)：vhost, 虚拟机, 可选参数，默认值为‘/’<br/>
返回值：无<br/>
**mq:conf(host, port, username, password[, vhost])**
<br/><br/>

* 连接broker <br/>
参数：无<br/>
返回值：失败：错误信息，成功：nil<br/>
**mq:connect()**
<br/><br/>

* 创建channel对象 <br/>
参数1(number, opt)：自定义channelId值，可选参数，默认不需要填入，系统自动生成<br/>
返回值： 1：channel对象，2：错误信息<br/>
成功：1:channel, 2:nil<br/>
失败：1：nil, 2:错误信息<br/>
**mq:connect([channelId])**
<br/><br/>

* 断开rabbitmq连接并销毁资源 <br/>
参数：无<br/>
返回值：失败：错误信息，成功：nil<br/>
**mq:disConnect()**
<br/><br/>


**channel对象成员方法，如对象名为 channel**
* 声明exchange <br/>
参数1(string)：exchange, 交换机名字<br/>
参数2(string)：exchange_type, 交换机类型, fanout 广播 direct直接绑定 topic主题模糊匹配 headers首部<br/>
参数3(boolean)：durable , true持久化 false非持久化<br/>
参数4(boolean)：auto_delete, true没有exchange绑定后删除队列  false不删除<br/>
返回值：失败：错误信息，成功：nil<br/>
**channel:exchangeDeclare(exchange, exchange_type, durable, auto_delete)**
<br/><br/>

* 声明queue <br/>
参数1(string/nil)：queue, 队列名字，如果没有名字可填nil，由系统分配一个名称，否则返回的队列名与参数1相同<br/>
参数2(boolean)：durable, true持久化 false非持久化<br/>
参数3(boolean)：exclusive , true当前连接不在时，队列自动删除  false当前连接不在时，队列不自动删除<br/>
参数4(boolean)：auto_delete, true没有consumer时，队列自动删除  false没有consumer时，队列不自动删除<br/>
返回值： 1：queue名字，2：错误信息<br/>
成功：1：queue名字, 2:nil<br/>
失败：1：nil, 2:错误信息<br/>
**channel:queueDeclare(queue, durable, exclusive, auto_delete)**
<br/><br/>

* 绑定交换机和队列 <br/>
参数1(string)：queue, 队列<br/>
参数2(string)：exchange, 交换机<br/>
参数3(string)：routing_key , 路由值<br/>
返回值：失败：错误信息，成功：nil<br/>
**channel:queueBind(queue, exchange, routing_key)**
<br/><br/>

* 关闭通道channel<br/>
参数：无<br/>
返回值：失败：错误信息，成功：nil<br/>
**channel:closeChannel()**
<br/><br/>

* QoS流控 <br/>
参数1(number)：prefetch_count, 每次抓取的消息条数<br/>
参数2(boolean)：global, true\false, 确认消息时，确认本条之前的所有消息(true)还是只确认本条(false)<br/>
返回值：失败：错误信息，成功：nil<br/>
**channel:basicQos(prefetch_count, global)**
<br/><br/>

* 开始一个queue consumer <br/>
参数1(string)：queue, 队列名字<br/>
参数2(string/nil)：consumer_tag, consumer标识 ，如果没有自定义可填nil<br/>
参数3(boolean)：no_local , true不接收 false接收<br/>
参数4(boolean)：no_ack, true不回复 false回复<br/>
参数4(boolean)：exclusive, true当前连接不在时，队列自动删除  false当前连接不在时，队列不自动删除<br/>
返回值：失败：错误信息，成功：nil<br/>
**channel:basicConsume(queue, consumer_tag, no_local, no_ack, exclusive)**
<br/><br/>

* 发布消息 <br/>
参数1(string)：exchange, 交换机<br/>
参数2(string)：routing_key, 路由键<br/>
参数3(boolean)：mandatory , true消息必须路由到存在的队列，否则返回失败<br/>
参数4(boolean)：immediate, true如果消息没有订阅，返回失败<br/>
参数5(string)：body, 消息体<br/>
返回值：失败：错误信息，成功：nil<br/>
**channel:basicPublish(exchange, routing_key, mandatory, immediate, body)**
<br/><br/>

* 接收消息 <br/>
用法1：<br/>
参数1(function)：callback wrapper function, 消息回调函数包裹函数（见示例）<br/>
参数2(function)：error function, 错误处理函数<br/>
参数3(function): callback function, 回调函数<br/>
可选参数：...<br/>
返回值：失败：错误信息，成功：nil(这是一个阻塞函数，除非出错或结束，否则一直不返回)<br/>
注意：如收到"quit"消息字符，该函数可正常退出<br/>
**channel:wait(callback_wrapper_function, function() print(debug.traceback()) end, callback_function, ...)**
<br/><br/>

* 接收消息 <br/>
用法2：<br/>
参数1(function)：callback function, 回调函数<br/>
参数2(function)：error function, 错误处理函数<br/>
可选参数：...<br/>
返回值：失败：错误信息，成功：nil(这是一个阻塞函数，除非出错或结束，否则一直不返回)<br/>
注意：如收到"quit"消息字符，该函数可正常退出<br/>
**channel:wait(callback_function, function() print(debug.traceback()) end, ...)**
<br/><br/>

#### 生产者示例
```lua
local rabbitmq = require "rabbitmq"
local mq = rabbitmq.newRabbitmq()
mq:conf("xxx.xxx.xxx.xxx", 5672, "admin", "xxxxxx")
local err = mq:connect()
if err then
    print("connect err = "..err)
end

local channel, err = mq:createChannel()
print("channel="..type(channel)..", err="..tostring(err))

err = channel:exchangeDeclare("xxxxxxx", "direct", true, false)
if err then
    print("exchangeDeclare err = "..err)
end

local queue, err = channel:queueDeclare("xxxxxxx", true, false, false)
if not queue then
    print("queueDeclare err = "..err)
end

err = channel:queueBind("xxxxxx", "xxxxxxx", "xxx")
if err then
    print("queueBind err = "..err)
end

err = channel:basicPublish("xxxxxx", "xxx", true, false, "{ \"a\" : 1, \"b\" : 2 }")
if err then
    print("publish err = "..err)
end    

err = channel:closeChannel()
if err then
    print("closeChannel err = "..err)
end

err = mq:disConnect()
if err then
    print("disConnect err = "..err)
end

channel = nil
mq = nil
```

<br/>
#### 消费者示例

```lua
local cjson = require "cjson"
local rabbitmq = require "rabbitmq"

local recvT = {}
function recvT:callback(msg)
    print("msg = "..msg)
    local _,tab = pcall(cjson.decode, msg)
    print(gDumpTable(tab))  --gDumpTable: print table function
	return true
end

mq:conf("xxx.xxx.xxx.xxx", 5672, "admin", "xxxxxx")
local err = mq:connect()
if err then
    print("connect err = "..err)
end

local channel, err = mq:createChannel()
print("channel="..type(channel)..", err="..tostring(err))

err = channel:exchangeDeclare("xxxxxxx", "direct", true, false)
if err then
    print("exchangeDeclare err = "..err)
end

local queue, err = channel:queueDeclare("xxxxxxx", true, false, false)
if not queue then
    print("queueDeclare err = "..err)
end

err = channel:queueBind("xxxxxx", "xxxxxxx", "xxx")
if err then
    print("queueBind err = "..err)
end

err = channel:basicQos(1, false)
if err then
    print("basicQos err = "..err)
end

err = channel:basicConsume("xxxx", nil, false, false, false)
if err then
	print("basicConsume err = "..err)
end

用法1：
channel:wait(
function(cb, ...)
    local status, ret = pcall(cb, ...)
    if not status then
        print("call cb error, do something")
    end
end, function() print(debug.traceback()) end, recvT.recv_msg, recvT)

用法2：
channel:wait(recvT.recv_msg, function() print(debug.traceback()) end, recvT)

err = channel:closeChannel()
if err then
    print("closeChannel err = "..err)
end

err = mq:disConnect()
if err then
    print("disConnect err = "..err)
end

channel = nil
mq = nil
```
