using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Caching;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using Newtonsoft.Json;

namespace RedisTest
{
    class Program
    {
        private const string KEY_STATE_SUFFIX = "_STATE";
        static void Main(string[] args)
        {
            var program = new Program();
            //ProcessBigData(program);
            //ProcessListData(program);
            //HashTest();
            TestGenericCall(program);
        }

        public static void TestGenericCall(Program program)
        {
            Customer customer1 = new Customer() { ID = 1, Name = "Benjamin" };
            Customer customer2 = new Customer() { ID = 2, Name = "Alfred" };
            Customer customer3 = new Customer() { ID = 3, Name = "Nancy" };
            Customer customer4 = new Customer() { ID = 4, Name = "Jacob" };

            IEnumerable<Customer> customers = new List<Customer>();
            ((List<Customer>)customers).Add(customer1);
            ((List<Customer>)customers).Add(customer2);
            ((List<Customer>)customers).Add(customer3);
            ((List<Customer>)customers).Add(customer4);

            //Test SET
            var itemsSet = program.AddOrGetExisting(new CacheItem("GenericKeyTest2", customers) { }, new CacheItemPolicy() { AbsoluteExpiration = DateTimeOffset.Now, SlidingExpiration = new TimeSpan() });

            //Test GET
            var itemsGet = program.AddOrGetExisting(new CacheItem("GenericKeyTest2", null) { }, new CacheItemPolicy() { AbsoluteExpiration = DateTimeOffset.Now, SlidingExpiration = new TimeSpan() });
            List<Customer> customersRetrieved = ((List<object>)itemsGet.Value).Cast<Customer>().ToList();
            //List<Customer> retrievedList = (List<Customer>)item.Value;
        }

        public static void ProcessBigData(Program program)
        {
            Console.WriteLine("Saving random data in cache");
            program.SaveBigData();

            Console.WriteLine("Reading data from cache");
            program.ReadBigData();

            Console.WriteLine("Removing big data in cache");
            program.RemoveBigData();

            Console.WriteLine("DONE");

            Console.ReadLine();
        }

        public static void ProcessListData(Program program)
        {
            Console.WriteLine("Saving list data in cache");
            program.SaveListData();

            Console.WriteLine("Reading list data in cache");
            program.ReadListData();

            Console.WriteLine("Removing list data in cache");
            program.RemoveListData();

            Console.WriteLine("DONE");

            Console.ReadLine();
        }

        public void ReadBigData()
        {
            var cache = RedisConnectorHelper.Connection.GetDatabase();
            var devicesCount = 10000;
            for (int i = 0; i < devicesCount; i++)
            {
                var value = cache.StringGet($"Device_Status:{i}");
                Console.WriteLine($"Valor={value}");
            }
        }

        public void SaveBigData()
        {
            var devicesCount = 10000;
            var rnd = new Random();
            var cache = RedisConnectorHelper.Connection.GetDatabase();

            for (int i = 1; i < devicesCount; i++)
            {
                var value = rnd.Next(0, 10000);
                cache.StringSet($"Device_Status:{i}", value);
            }
        }

        public void RemoveBigData()
        {
            var devicesCount = 10000;
            var cache = RedisConnectorHelper.Connection.GetDatabase();

            for (int i = 1; i < devicesCount; i++)
            {
                cache.KeyDelete($"Device_Status:{i}");
            }
        }

        public void SaveListData()
        {
            var cache = RedisConnectorHelper.Connection.GetDatabase();

            Customer customer1 = new Customer() { ID = 1, Name = "Benjamin" };
            Customer customer2 = new Customer() { ID = 2, Name = "Alfred" };
            Customer customer3 = new Customer() { ID = 3, Name = "Nancy" };
            Customer customer4 = new Customer() { ID = 4, Name = "Jacob" };

            IEnumerable<Customer> customers = new Customer[]{customer3, customer4, customer1, customer2};

            //cache.ListRightPush("Customers", customers.Select(x => (RedisValue)x.ToString()).ToArray());
            //cache.ListRightPush("Customers", customers.Select(x => (RedisValue)JsonConvert.SerializeObject(x)).ToArray());
            
            foreach (var customer in customers)
            {
                cache.ListRightPush("Customers", JsonConvert.SerializeObject(customer), When.Always, CommandFlags.None);
            }
            
        }

        public void ReadListData()
        {
            var cache = RedisConnectorHelper.Connection.GetDatabase();
            RedisValue[] customers = cache.ListRange("Customers");

            foreach (RedisValue customer in customers)
            {
                Console.WriteLine($"Customer={customer.ToString()}");
            }

            cache.SortAndStore("CustomersSorted", "Customers", 0, -1, Order.Ascending, SortType.Alphabetic);
            RedisValue[] customersSorted = cache.ListRange("CustomersSorted");

            foreach (RedisValue customer in customersSorted)
            {
                Console.WriteLine($"Customer={customer.ToString()}");
            }
        }

        public void RemoveListData()
        {
            var cache = RedisConnectorHelper.Connection.GetDatabase();
            cache.KeyDelete($"Customers");
            cache.KeyDelete($"CustomersSorted");
        }

        public static void HashTest()
        {
            var redis = RedisConnectorHelper.Connection.GetDatabase();
            var hashKey = "hashKey";

            HashEntry[] redisBookHash = {
                new HashEntry("title", "Redis for .NET Developers"),
                new HashEntry("year", 2016),
                new HashEntry("author", "Taswar Bhatti")
            };

            redis.HashSet(hashKey, redisBookHash);

            if (redis.HashExists(hashKey, "year"))
            {
                var year = redis.HashGet(hashKey, "year"); //year is 2016
            }

            var allHash = redis.HashGetAll(hashKey);

            //get all the items
            foreach (var item in allHash)
            {
                //output 
                //key: title, value: Redis for .NET Developers
                //key: year, value: 2016
                //key: author, value: Taswar Bhatti
                Console.WriteLine($"key : {item.Name}, value : {item.Value}");
            }

            //get all the values
            var values = redis.HashValues(hashKey);

            foreach (var val in values)
            {
                Console.WriteLine(val); //result = Redis for .NET Developers, 2016, Taswar Bhatti
            }

            //get all the keys
            var keys = redis.HashKeys(hashKey);

            foreach (var k in keys)
            {
                Console.WriteLine(k); //result = title, year, author
            }

            var len = redis.HashLength(hashKey);  //result of len is 3

            if (redis.HashExists(hashKey, "year"))
            {
                var year = redis.HashIncrement(hashKey, "year", 1); //year now becomes 2017
                //var year2 = redis.HashDecrement(hashKey, "year", 1.5); //year now becomes 2015.5
            }

            Console.ReadLine();
        }

        public CacheItem AddOrGetExisting(CacheItem item, CacheItemPolicy policy)
        {
            var cache = RedisConnectorHelper.Connection.GetDatabase();

            //Determine if a Type is given for the Value in the CacheItem
            //string myType = "RedisTest.Customer";
            //Type itemValueType = item.Value.GetType();

            //if (itemValueType.IsGenericType)
            //{
                //Determine if iEnumerable<T>
                //if (IsEnumerable<T> ())
                //if (typeof(IEnumerable).IsAssignableFrom(itemValueType))
                //{
                    //If key exists, then retrieve data...
                    if (cache.KeyExists((RedisKey)item.Key))
                     {
                        if (cache.KeyType((RedisKey)item.Key) == RedisType.List)
                        {
                            string itemType = cache.StringGet(item.Key + KEY_STATE_SUFFIX);

                            var list = cache.ListRange(item.Key);
                            //List<Customer> customers = new List<Customer>();
                            var customerObjects = cache.ListRange(item.Key).Select(x => JsonConvert.DeserializeObject(x, Type.GetType(itemType))).ToList();
                            /*
                            foreach (object customerObject in customerObjects)
                            {
                                customers.Add((Customer)customerObject);
                            }
                            */
                            //testData.Cast<List<object>>().Select(x => new TestClass() { AAA = (string)x[0], BBB = (string)x[1], CCC = (string)x[2] }).ToList()
                            return new CacheItem(item.Key, customerObjects);
                            //return new CacheItem(item.Key, cache.ListRange(item.Key).Select(x => JsonConvert.DeserializeObject(x, Type.GetType(myType))).ToList());
                            //return new CacheItem(item.Key, cache.ListRange(item.Key).Select(x => JsonConvert.DeserializeObject(x, Type.GetType(itemValueType.GetGenericArguments()[0].UnderlyingSystemType.FullName))).ToList());
                        }
                    }
                    else
                    {
                        //If key doesn't exist, then add data...
                        if (item.Value is IEnumerable)
                        {
                            List<RedisValue> redisValueList = new List<RedisValue>();
                            string itemType = String.Empty;

                            foreach (var value in (IEnumerable)item.Value)
                            {
                                if (string.IsNullOrEmpty(itemType))
                                {
                                    itemType = value.GetType().FullName;
                                }

                                redisValueList.Add((RedisValue)JsonConvert.SerializeObject(value));
                            }

                            //cache.SetAdd(item.Key + KEY_STATE_SUFFIX, ((IEnumerable)item.Value).GetEnumerator().Current.GetType().FullName, CommandFlags.FireAndForget);
                            cache.StringSet(item.Key + KEY_STATE_SUFFIX, itemType); //null, When.Always, CommandFlags.FireAndForget;
                            cache.ListRightPush(item.Key, redisValueList.ToArray(), CommandFlags.FireAndForget);
                            //cache.ListRightPush(item.Key, ((IEnumerable)item.Value).Select(x => (RedisValue)JsonConvert.SerializeObject(x)).ToArray(), CommandFlags.FireAndForget));
                        }
                    }
                //}
            //}

            return item;
        }
    }
}
