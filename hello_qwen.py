import os
from dashscope import Generation
import dashscope
# Note: The base_url is different for each region. The example below uses the base_url for the Singapore region.
# - Singapore: https://dashscope-intl.aliyuncs.com/api/v1
# - US (Virginia): https://dashscope-us.aliyuncs.com/api/v1
# - China (Beijing): https://dashscope.aliyuncs.com/api/v1
dashscope.base_http_api_url = 'https://dashscope-intl.aliyuncs.com/api/v1'

messages = [
    {'role': 'system', 'content': 'You are a helpful assistant.'},
    {'role': 'user', 'content': 'Who are you?'}
    ]
response = Generation.call(
    # API keys for the Singapore, US (Virginia), and China (Beijing) regions are not interchangeable. Get API Key: https://www.alibabacloud.com/help/model-studio/get-api-key
    # If the environment variable is not configured, replace the following line with: api_key = "sk-xxx", using your Model Studio API key.
    api_key=os.getenv("DASHSCOPE_API_KEY"), 
    model="qwen-plus",   
    messages=messages,
    result_format="message"
)

if response.status_code == 200:
    print(response.output.choices[0].message.content)
else:
    print(f"HTTP status code: {response.status_code}")
    print(f"Error code: {response.code}")
    print(f"Error message: {response.message}")
    print("For more information, see the documentation: https://www.alibabacloud.com/help/zh/model-studio/developer-reference/error-code")