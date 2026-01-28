# import dashscope
# from http import HTTPStatus

# # PASTE YOUR NEW KEY DIRECTLY HERE FOR THIS TEST
# # (Do not use os.getenv yet, let's eliminate the .env file as a variable)
# dashscope.api_key = "YOUR-KEY" 

# def test_qwen():
#     try:
#         response = dashscope.Generation.call(
#             model='Qwen-2.5-Coder-32B',
#             messages=[{'role': 'user', 'content': 'Say "Connection Successful"'}]
#         )
        
#         if response.status_code == HTTPStatus.OK:
#             print("✅ SUCCESS!")
#             print(response.output.text)
#         else:
#             print("❌ API ERROR:")
#             print(f"Code: {response.code}")
#             print(f"Message: {response.message}")
            
#     except Exception as e:
#         print(f"❌ EXCEPTION: {e}")

# if __name__ == "__main__":
#     test_qwen()

from app.services.vanna_wrapper import vn

# 1. Train it with ONE simple rule (to test storage)
vn.train(ddl="CREATE TABLE test_table (id INT, name TEXT);")

# 2. Ask a question (to test Qwen connection)
sql = vn.generate_sql("Show me all names from test table")

print(sql) 
# Expect: SELECT name FROM test_table;