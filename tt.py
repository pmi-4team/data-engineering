import redis
r = redis.StrictRedis(host="localhost", port=6379, db=0, decode_responses=True)
print(r.hget("synonym_rules", "만족도"))
print(r.hget("dictionary_rules:SYNONYM", "만족도"))
