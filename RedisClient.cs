using StackExchange.Redis;


namespace RedisClientHandler
{
    
    public class RedisClient
    {

        public delegate void RedisClientSubscriberEvents(string channel, string message);
        public event RedisClientSubscriberEvents? OnKeyExpired;

        public delegate void RedisClientConnectionEvents(string message);
        public event RedisClientConnectionEvents? ConnectionFailed;
        public event RedisClientConnectionEvents? ConnectionRestored;


        static ConnectionMultiplexer redis;
        //static readonly ConnectionMultiplexer redisKeyExpireClient = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { $"{Configurations.Instance.RedisConnectorSettings["ip"]}:{Configurations.Instance.RedisConnectorSettings["port"]}" }, Password = Configurations.Instance.RedisConnectorSettings["auth"], });

        int dbId;
        private IDatabase GetDatabase()
        {
            return redis.GetDatabase(dbId);
        }

        private Task<bool> EnableKeyExpiredSubscriber()
        {
            IDatabase db = GetDatabase();
            db.PingAsync().Wait();
            ISubscriber sub = redis.GetSubscriber();
            sub.Subscribe("__key*__:expired", (channel, message) =>
            {
                OnKeyExpired?.Invoke(channel, message);
            });
            return Task.FromResult(true);
        }

        public RedisClient(int databaseID, string serverIP, string serverPort, string password)
        {
            dbId = databaseID;
            redis = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { $"{serverIP}:{serverPort}" }, Password = password , AbortOnConnectFail= false, ConnectTimeout = 5000});
            //redis = ConnectionMultiplexer.Connect(new ConfigurationOptions()
            //{
            //    EndPoints = { $"{serverIP}:{serverPort}" },
            //    AbortOnConnectFail = false
            //});
            redis.ConnectionFailed += (sender, args) =>
            {
                ConnectionFailed?.Invoke($"Connection failed : {args?.Exception?.Message}");
            };

            redis.ConnectionRestored += (sender, args) =>
            {
                ConnectionRestored?.Invoke($"Connection restored : {args?.Exception?.Message}");
            };
        }

        
        


        /// <summary>
        /// Obtain a pub/sub subscriber connection to the specified server.
        /// </summary>
        /// <param name="asyncState">The async state object to pass to the created <see cref="RedisSubscriber"/>.</param>
        //public Task<ISubscriber> GetSubscriber()
        //{
        //    IDatabase db = GetDatabase();
        //    db.PingAsync().Wait();
        //    ISubscriber sub = redis.GetSubscriber();
        //    return Task.FromResult(sub);
        //}

        /// <summary>
        /// Returns if key exists.
        /// </summary>
        /// <param name="key">The key to check.</param>
        /// <returns><see langword="true"/> if the key exists. <see langword="false"/> if the key does not exist.</returns>
        public Task<bool> KeyExistsAsync(string key)
        {
            var db = GetDatabase();
            return db.KeyExistsAsync(key);
        }

        /// <summary>
        /// Insert the specified value at the tail of the list stored at key.
        /// If key does not exist, it is created as empty list before performing the push operation.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="value">The value to add to the tail of the list.</param>
        /// <returns>The length of the list after the push operation.</returns>
        public Task<long> PushMessage(string key, string value)
        {
            var db = GetDatabase();
            return db.ListRightPushAsync(key, value);
        }


        /// <summary>
        /// Removes and returns the first element of the list stored at key.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <returns>The value of the first element, or nil when key does not exist.</returns>
        public Task<RedisValue> PopMessage(string key)
        {
            var db = GetDatabase();
            return db.ListLeftPopAsync(key);
        }


        /// <summary>
        /// Set key to hold the string value. If key already holds a value, it is overwritten, regardless of its type.
        /// </summary>
        /// <param name="key">The key of the string.</param>
        /// <param name="value">The value to set.</param>
        /// <returns><see langword="true"/> if the string was set, <see langword="false"/> otherwise.</returns>
        public Task<bool> Save(string key, string value)
        {
            var db = GetDatabase();
            return db.StringSetAsync(key, value);
        }

        /// <summary>
        /// Set key to hold the string value. If key already holds a value, it is overwritten, regardless of its type.
        /// </summary>
        /// <param name="key">The key of the string.</param>
        /// <param name="value">The value to set.</param>
        /// <param name="ttlValue">The expiry to set.</param>
        /// <returns><see langword="true"/> if the string was set, <see langword="false"/> otherwise.</returns>
        public Task<bool> SaveExpires(string key, string value, int ttlValue)
        {
            var expires =  TimeSpan.FromHours(ttlValue);// TimeSpan.FromSeconds(30);// TimeSpan.FromHours(sessionTTL);
            var db = GetDatabase();
            return db.StringSetAsync(key, value, expires);
            //RedisKey key, RedisValue value, TimeSpan? expiry = null, bool keepTtl = false, When when = When.Always, CommandFlags flags = CommandFlags.None
        }

        /// <summary>
        ///   Removes the specified key. A key is ignored if it does not exist. 
        /// </summary>
        /// <param name="key">  The key to delete.</param>
        /// <returns>true if the key was removed.</returns>
        public Task<bool> Delete(string key)
        {
            var db = GetDatabase();
            return db.KeyDeleteAsync(key);
        }


        /// <summary>
        /// Get the value of key. If the key does not exist the special value nil is returned.
        /// An error is returned if the value stored at key is not a string, because GET only handles string values.
        /// </summary>
        /// <param name="key">The key of the string.</param>
        /// <returns>The value of key, or nil when key does not exist.</returns>
        public Task<RedisValue> GetAsync(string key)
        {
            var db = GetDatabase();
            return db.StringGetAsync(key);
        }
    }
}