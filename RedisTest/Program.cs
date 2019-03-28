using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
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
            Program program = new Program();
            //ProcessBigData(program);
            //ProcessListData(program);
            //HashTest();
            //TestGenericCall(program);
            //TestListAsObject(program);
            RemoveKeys(program);
        }

        public static void RemoveKeys(Program program)
        {
            //var cache = RedisConnectorHelper.Connection.GetDatabase();
            //cache.KeyDelete("Generic*");
            //EndPoint[] endpoints = RedisConnectorHelper.Connection.GetEndPoints(true);
            IServer server = RedisConnectorHelper.Connection.GetServer("d-portsolapp01:6379");

            // show all keys in database 0 that include "foo" in their name
            foreach (RedisKey key in server.Keys(pattern: "GenericKeyTest2*"))
            {
                Console.WriteLine(key);
            }

            Console.ReadLine();
            // completely wipe ALL keys from database 0
            // server.FlushDatabase();
        }

        public static void TestListAsObject(Program program)
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
            IDatabase cache = RedisConnectorHelper.Connection.GetDatabase();
            cache.StringSet("GenericKeyTest3", JsonConvert.SerializeObject(customers));

            //Test GET
            RedisValue cacheString = cache.StringGet("GenericKeyTest3");
            IEnumerable<Customer> customersRetrieved = JsonConvert.DeserializeObject<IEnumerable<Customer>>(cacheString);
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
            CacheItem itemsSet = program.AddOrGetExisting(new CacheItem("GenericKeyTest2", customers) { }, new CacheItemPolicy() { AbsoluteExpiration = DateTimeOffset.Now, SlidingExpiration = new TimeSpan() });

            List<Customer> customerList = (List<Customer>)itemsSet.Value;

            //Test GET
            CacheItem itemsGet = program.AddOrGetExisting(new CacheItem("GenericKeyTest2", null) { }, new CacheItemPolicy() { AbsoluteExpiration = DateTimeOffset.Now, SlidingExpiration = new TimeSpan() });
            List<Customer> customersRetrieved = (List<Customer>)itemsGet.Value;
            //List<Customer> customersRetrieved = ((List<object>)itemsGet.Value).Cast<Customer>().ToList();
            //List<Customer> retrievedList = (List<Customer>)item.Value;
        }

        public static void ProcessBigData(Program program)
        {
            Console.WriteLine("Saving random data in cache");
            program.SaveBigData();

            Console.WriteLine("Reading data from cache");
            //program.ReadBigData();

            Console.WriteLine("Removing big data in cache");
            //program.RemoveBigData();

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
            IDatabase cache = RedisConnectorHelper.Connection.GetDatabase();
            int devicesCount = 10000;
            for (int i = 0; i < devicesCount; i++)
            {
                RedisValue value = cache.StringGet($"Device_Status:{i}");
                Console.WriteLine($"Valor={value}");
            }
        }

        public void SaveBigData()
        {
            int devicesCount = 10000;
            Random rnd = new Random();
            IDatabase cache = RedisConnectorHelper.Connection.GetDatabase();

            for (int i = 1; i < devicesCount; i++)
            {
                int value = rnd.Next(0, 10000);
                cache.StringSet($"Device_Status:{i}", value);
            }
        }

        public void RemoveBigData()
        {
            int devicesCount = 10000;
            IDatabase cache = RedisConnectorHelper.Connection.GetDatabase();

            for (int i = 1; i < devicesCount; i++)
            {
                cache.KeyDelete($"Device_Status:{i}");
            }
        }

        public void SaveListData()
        {
            IDatabase cache = RedisConnectorHelper.Connection.GetDatabase();

            Customer customer1 = new Customer() { ID = 1, Name = "Benjamin" };
            Customer customer2 = new Customer() { ID = 2, Name = "Alfred" };
            Customer customer3 = new Customer() { ID = 3, Name = "Nancy" };
            Customer customer4 = new Customer() { ID = 4, Name = "Jacob" };

            IEnumerable<Customer> customers = new Customer[]{customer3, customer4, customer1, customer2};

            //cache.ListRightPush("Customers", customers.Select(x => (RedisValue)x.ToString()).ToArray());
            //cache.ListRightPush("Customers", customers.Select(x => (RedisValue)JsonConvert.SerializeObject(x)).ToArray());
            
            foreach (Customer customer in customers)
            {
                cache.ListRightPush("Customers", JsonConvert.SerializeObject(customer), When.Always, CommandFlags.None);
            }
            
        }

        public void ReadListData()
        {
            IDatabase cache = RedisConnectorHelper.Connection.GetDatabase();
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
            IDatabase cache = RedisConnectorHelper.Connection.GetDatabase();
            cache.KeyDelete($"Customers");
            cache.KeyDelete($"CustomersSorted");
        }

        public static void HashTest()
        {
            IDatabase redis = RedisConnectorHelper.Connection.GetDatabase();
            string hashKey = "hashKey";

            HashEntry[] redisBookHash = {
                new HashEntry("title", "Redis for .NET Developers"),
                new HashEntry("year", 2016),
                new HashEntry("author", "Taswar Bhatti")
            };

            redis.HashSet(hashKey, redisBookHash);

            if (redis.HashExists(hashKey, "year"))
            {
                RedisValue year = redis.HashGet(hashKey, "year"); //year is 2016
            }

            HashEntry[] allHash = redis.HashGetAll(hashKey);

            //get all the items
            foreach (HashEntry item in allHash)
            {
                //output 
                //key: title, value: Redis for .NET Developers
                //key: year, value: 2016
                //key: author, value: Taswar Bhatti
                Console.WriteLine($"key : {item.Name}, value : {item.Value}");
            }

            //get all the values
            RedisValue[] values = redis.HashValues(hashKey);

            foreach (RedisValue val in values)
            {
                Console.WriteLine(val); //result = Redis for .NET Developers, 2016, Taswar Bhatti
            }

            //get all the keys
            RedisValue[] keys = redis.HashKeys(hashKey);

            foreach (RedisValue k in keys)
            {
                Console.WriteLine(k); //result = title, year, author
            }

            long len = redis.HashLength(hashKey);  //result of len is 3

            if (redis.HashExists(hashKey, "year"))
            {
                long year = redis.HashIncrement(hashKey, "year", 1); //year now becomes 2017
                //var year2 = redis.HashDecrement(hashKey, "year", 1.5); //year now becomes 2015.5
            }

            Console.ReadLine();
        }

        public CacheItem AddOrGetExisting(CacheItem item, CacheItemPolicy policy)
        {
            IDatabase cache = RedisConnectorHelper.Connection.GetDatabase();

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

                            RedisValue[] list = cache.ListRange(item.Key);
                            //List<Customer> customers = new List<Customer>();
                            List<object> customerObjects = cache.ListRange(item.Key).Select(x => JsonConvert.DeserializeObject(x, Type.GetType(itemType))).ToList();
                            object redisListConverted = ConvertList(customerObjects, Type.GetType(itemType));

                            /*
                            foreach (object customerObject in customerObjects)
                            {
                                customers.Add((Customer)customerObject);
                            }
                            */
                            //testData.Cast<List<object>>().Select(x => new TestClass() { AAA = (string)x[0], BBB = (string)x[1], CCC = (string)x[2] }).ToList()
                            return new CacheItem(item.Key, redisListConverted); //customerObjects
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

                            foreach (object value in (IEnumerable)item.Value)
                            {
                                if (string.IsNullOrEmpty(itemType))
                                {
                                    //itemType = value.GetType().FullName;
                                    itemType = value.GetType().AssemblyQualifiedName;
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

        /*
        public static object ConvertList(List<object> value, Type type)
        {
            //var containedType = type.GenericTypeArguments.First();
            //return value.Select(item => Convert.ChangeType(item, containedType)).ToList();
            return value.Select(item => Convert.ChangeType(item, type)).ToList();
        }
        */

        public static object ConvertList(List<object> items, Type type, bool performConversion = false)
        {
            Type containedType = type; //type.GenericTypeArguments.First();
            Type enumerableType = typeof(System.Linq.Enumerable);
            MethodInfo castMethod = enumerableType.GetMethod(nameof(System.Linq.Enumerable.Cast)).MakeGenericMethod(containedType);
            MethodInfo toListMethod = enumerableType.GetMethod(nameof(System.Linq.Enumerable.ToList)).MakeGenericMethod(containedType);

            IEnumerable<object> itemsToCast;

            if (performConversion)
            {
                itemsToCast = items.Select(item => Convert.ChangeType(item, containedType));
            }
            else
            {
                itemsToCast = items;
            }

            object castedItems = castMethod.Invoke(null, new[] { itemsToCast });

            return toListMethod.Invoke(null, new[] { castedItems });
        }
    }
}
