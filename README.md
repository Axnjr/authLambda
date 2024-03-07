### A Lambda function to authenticate and rate limit the incomming user request's according to their respective subscriptions in the background without increasing the overall latency for my new Real time SAAS Spark.
### Spark will have 3 plans viz:

  - Hobby (Free): 100 Daily Requests
  - Priority: 5000 Daily Requests
  - Enterprize: Infinite Daily Requests ( Not planned lmao 😅 )

### However after running a few benchmark tests I was disappointed. Rust lambda's are not that fast & efficient. 
### Processing Each request took around **2s** each that's indeed fast for what I am doing but I still felt that it could be more fast, so I researched and found that: Lambda's have some issues with Tokio Async Runtime.

# Solution
I re-wrote it in Go & now each request takes around **800ms** 🚀🤘 https://github.com/Axnjr/authLambdaGo
