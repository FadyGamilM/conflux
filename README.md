# conflux
conflux is a distributed messaging queue system which written entirely in golang

## main supported features :
- at least once delivery mechanism
- batch processing which means processing batch of data events and if one of them is an error, we couldn't retry, and batch processing means processing of data in batches rather than processing individual messages in real-time.

# Version.1.0.0
➜ Request(s) in order <br>
```json
    {
        "data": "password:123456789" 
    }

```
```json
    {
       "data": "password:123456789" 
    }
```

➜ Response <br>
```json
    {
        "response": "username:fadyg\npassword:123456789\n"
    }
```