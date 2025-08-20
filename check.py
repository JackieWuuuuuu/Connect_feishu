import asyncio
import lark_oapi as lark
import requests
import json
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

# 飞书应用凭证
APP_ID = "***"
APP_SECRET = "***"

# 创建线程池执行器用于运行同步IO操作
executor = ThreadPoolExecutor(max_workers=10)

async def get_dify_output(message: str) -> Optional[str]:
    loop = asyncio.get_event_loop()
    try:
        url = "***"
        headers = {"Content-Type": "application/json", "Authorization": "***"}
        data = {
            "inputs": {},
            "query": message,
            "response_mode": "blocking",
            "conversation_id": "",
            "user": "admin",
            "files": [{
                "type": "image",
                "transfer_method": "remote_url",
                "url": "***"
            }]
        }
        
        # 将同步的requests调用转为异步
        response = await loop.run_in_executor(
            executor,
            lambda: requests.post(url, headers=headers, json=data, timeout=5000)
        )
        return response.json().get("answer")
    except Exception as e:
        print(f"获取 dify_output 失败: {e}")
        return None

async def get_tenant_access_token() -> Optional[str]:
    loop = asyncio.get_event_loop()
    try:
        url = "***"
        headers = {"Content-Type": "application/json"}
        data = {"app_id": APP_ID, "app_secret": APP_SECRET}
        
        # 将同步的requests调用转为异步
        response = await loop.run_in_executor(
            executor,
            lambda: requests.post(url, headers=headers, json=data, timeout=5000)
        )
        return response.json().get("tenant_access_token")
    except Exception as e:
        print(f"获取 tenant_access_token 失败: {e}")
        return None

async def send_text_message(receive_id: str, text: str) -> bool:
    loop = asyncio.get_event_loop()
    token = await get_tenant_access_token()
    if not token:
        return False
        
    # 自动判断 receive_id 类型
    if receive_id.startswith("ou_"):
        receive_id_type = "open_id"
    elif receive_id.startswith("oc_"):
        receive_id_type = "chat_id"
    elif "@" in receive_id:
        receive_id_type = "email"
    else:
        receive_id_type = "open_id"  # 默认尝试
        
    url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json;charset=utf-8"
    }
    data = {
        "receive_id": receive_id,
        "msg_type": "text",
        "content": json.dumps({"text": text})
    }
    
    try:
        # 将同步的requests调用转为异步
        response = await loop.run_in_executor(
            executor,
            lambda: requests.post(url, headers=headers, json=data, timeout=5000)
        )
        print(f"飞书API响应状态码: {response.status_code}")
        print(f"飞书API响应内容: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"发送消息失败: {str(e)}")
        return False

async def process_message_async(message_content: str, sender_id: str, chat_id: str, chat_type: str):
    try:
        # 异步获取Dify回复
        dify_response = await get_dify_output(message_content)
        if not dify_response:
            dify_response = "抱歉，我现在无法处理您的请求。"
            
        # 异步发送回复
        if chat_type == "p2p":  # 私聊
            await send_text_message(sender_id, dify_response)
        else:
            await send_text_message(chat_id, dify_response)
    except Exception as e:
        print(f"处理消息时出错: {e}")

def do_p2_im_message_receive_v1(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    message = data.event.message
    sender = data.event.sender
    print(f'[ do_p2_im_message_receive_v1 access ], data: {lark.JSON.marshal(data, indent=4)}')
    
    # 获取消息内容
    try:
        message_content = json.loads(message.content)["text"]
    except:
        message_content = message.content
    
    # 创建异步任务处理消息，不等待结果
    asyncio.create_task(
        process_message_async(
            message_content=message_content,
            sender_id=sender.sender_id.open_id,
            chat_id=message.chat_id,
            chat_type=message.chat_type
        )
    )

def do_message_event(data: lark.CustomizedEvent) -> None:
    print(f'[ do_customized_event access ], type: message, data: {lark.JSON.marshal(data, indent=4)}')

event_handler = lark.EventDispatcherHandler.builder("", "") \
    .register_p2_im_message_receive_v1(do_p2_im_message_receive_v1) \
    .register_p1_customized_event("这里填入你要自定义订阅的 event 的 key，例如 out_approval", do_message_event) \
    .build()

def main():
    # 创建事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # 启动飞书客户端
    cli = lark.ws.Client(APP_ID, APP_SECRET,
                         event_handler=event_handler,
                         log_level=lark.LogLevel.DEBUG)
    cli.start()

if __name__ == "__main__":

    main()
