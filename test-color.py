import okx.Account as Account

# API 初始化
apikey = "fc644348-d5e8-4ae1-8634-1aa3562011bb"
secretkey = "50FA45723B520A43571064FAD1F6598C"
passphrase = "12345678Zha."

flag = "0"  # 实盘:0 , 模拟盘:1

accountAPI = Account.AccountAPI(apikey, secretkey, passphrase, False, flag)

result = accountAPI.get_instruments(instType="SWAP")
print(result)
