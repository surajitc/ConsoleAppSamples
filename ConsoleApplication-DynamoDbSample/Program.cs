using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;

namespace ConsoleApplication_DynamoDbSample
{
    public class MessageProps
    {
        public string TransOrigin { get; set; }
        public string MessageOrigin { get; set; }
        public string MessageDestination { get; set; }
        public string MessageType { get; set; }

        public string AppId { get; set; }

        public string LocatorPrefix { get; set; }

    }

    public class Program
    {
        private static string tableName = "ExampleTable";
        private static string transLogTableName = "TransactionLogTable";
       

        public static KeyValuePair<string, string>[] Parameters { get; set; }

        public static string ProfilesLocation => Parameters.Select(x =>
        {
            if (x.Key == "profilesLocation")
                return x;

            return new KeyValuePair<string, string>("", "credentials.ini");
        }).First().Value;

        public static string ProfileName
            => Parameters.FirstOrDefault(x => x.Key == "profileName").Value;

        public static string Region => Parameters.Select(x =>
        {
            if (x.Key == "region")
                return x;
            return new KeyValuePair<string, string>("", RegionEndpoint.USEast1.SystemName);
        }).First().Value;

        static void Main(string[] args)
        {
            DynamoDBContext context = null;
            AmazonDynamoDBClient client = null;
            byte[] reqBytes;
            byte[] responseBytes;

            try
            {
                
                //Console.WriteLine("Transaction table pre-created - hit enter to delete all");
                //Console.ReadLine();

                //Delete(context);
                Console.WriteLine($"Directory = {Directory.GetCurrentDirectory()}");
                var path = ReadFilePath();
                reqBytes = ReadFile(path, "NiemMessageBody6.xml");
                responseBytes = ReadFile(path, "NiemMessageBody5.xml");
                Console.ReadLine();

                client = SetupDynamo();
                // var credentials = InitializeProfile();
                //CreateExampleTable(client);
                //Console.WriteLine("Example table created - hit enter to continue");
                //Console.ReadLine();
                //CreateTransactionLogTable(client);

                context = GetContext(client);

                Console.WriteLine("Transaction table pre-created - hit enter to continue and save 0.5 million records");
                Console.ReadLine();

                for (int i = 0; i < 500000; i++)
                {
                    WriteToTransLog(context, i, reqBytes, responseBytes);
                    Console.WriteLine($"Wrote record number {i}");
                }
                Console.WriteLine("Transaction table populated with 500000 records - hit enter to continue and read");
                Console.ReadLine();

                ReadFromTransLogUsingPrimaryIndex(context);

                Console.WriteLine("Read done using primary index - hit enter to continue and read using GSI");
                Console.ReadLine();

                ReadFromTransLogUsingGlobalSecondaryIndex(context);


                Console.WriteLine("Read done using GSI - hit enter to continue and read using LSI");
                Console.ReadLine();
                ReadFromTransLogUsingLocalSecondaryIndex(context);

            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
            }
            finally
            {
                context?.Dispose();
                client?.Dispose();

            }

            Console.ReadLine();
        }

        public static AWSCredentials InitializeProfile()
        {
            var chain = new CredentialProfileStoreChain(ProfilesLocation);
            AWSCredentials awsCredentials;
            if (chain.TryGetAWSCredentials(ProfileName, out awsCredentials))
            {
                return awsCredentials;
            }

            throw new Exception($"Credentinal file {ProfilesLocation} doesnot contain profile {ProfileName}");
        }

        public static AmazonDynamoDBClient SetupDynamo()
        {
            AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig {RegionEndpoint = RegionEndpoint.USEast1};
            // This client will access the US East 1 region.

            AmazonDynamoDBClient client = new AmazonDynamoDBClient(clientConfig);


            return client;
        }

        private static DynamoDBContext GetContext(AmazonDynamoDBClient client)
        {
            DynamoDBContext dbContext = new DynamoDBContext(client);
            return dbContext;
        }

        private static string ReadFilePath()
        {
            string path = Directory.GetCurrentDirectory();
            string xmlfilePath = path + "\\xml";
            Console.WriteLine($"{xmlfilePath}");
            return xmlfilePath;
        }

        private static byte[] ReadFile(string path, string filename)
        {
            string xmlPath = path + "\\" + filename;
             
            byte[] contBytes = File.ReadAllBytes(xmlPath);

            return contBytes;
        }
        private static void CreateTransactionLogTable(AmazonDynamoDBClient client)
        {
            Console.WriteLine("\n*** Creating Transaction Log table ***");
            var request = new CreateTableRequest()
            {
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition()
                    {
                        AttributeName = "Id",
                        AttributeType = "S"
                    },

                    new AttributeDefinition()
                    {
                        AttributeName = "TranOrigin",
                        AttributeType = "S"
                    },

                },
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement()
                    {
                        AttributeName = "Id",
                        KeyType = "HASH" //Partition key
                    },
                    new KeySchemaElement()
                    {
                        AttributeName = "TranOrigin",
                        KeyType = "RANGE" //Sort key
                    }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5,
                    WriteCapacityUnits = 6
                },
                TableName = transLogTableName
            };

            var response = client.CreateTable(request);

            var tableDescription = response.TableDescription;
            Console.WriteLine("{1}: {0} \t ReadsPerSec: {2} \t WritesPerSec: {3}",
                tableDescription.TableStatus,
                tableDescription.TableName,
                tableDescription.ProvisionedThroughput.ReadCapacityUnits,
                tableDescription.ProvisionedThroughput.WriteCapacityUnits);

            string status = tableDescription.TableStatus;
            Console.WriteLine(tableName + " - " + status);

            WaitUntilTableReady(client, transLogTableName);
        }

        private static void CreateExampleTable(AmazonDynamoDBClient client)
        {
            Console.WriteLine("\n*** Creating table ***");
            var request = new CreateTableRequest
            {
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Id",
                        AttributeType = "S"
                    },
                    new AttributeDefinition
                    {
                        AttributeName = "LogValue",
                        AttributeType = "S"
                    },
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Id",
                        KeyType = "HASH" //Partition key
                    },
                    new KeySchemaElement
                    {
                        AttributeName = "LogValue",
                        KeyType = "RANGE" //Sort key
                    }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5,
                    WriteCapacityUnits = 6
                },
                TableName = tableName
            };

            var response = client.CreateTable(request);

            var tableDescription = response.TableDescription;
            Console.WriteLine("{1}: {0} \t ReadsPerSec: {2} \t WritesPerSec: {3}",
                tableDescription.TableStatus,
                tableDescription.TableName,
                tableDescription.ProvisionedThroughput.ReadCapacityUnits,
                tableDescription.ProvisionedThroughput.WriteCapacityUnits);

            string status = tableDescription.TableStatus;
            Console.WriteLine(tableName + " - " + status);

            WaitUntilTableReady(client, tableName);
        }

        private static void WriteToTransLog(DynamoDBContext dbContext, int i, byte[] reqBytes, byte[] responseBytes)
        {
            Random _randomOps, _randomProps;


            List<MessageProps> _messageProps = new List<MessageProps>()
            {
                new MessageProps()
                {
                    TransOrigin = "A4",
                    MessageOrigin = "A4",
                    MessageType = "UA",
                    MessageDestination = "XX",
                    AppId = "37",
                    LocatorPrefix = ConfigurationManager.AppSettings["LocatorPrefix1"]
                },
                new MessageProps()
                {
                    TransOrigin = "A4",
                    MessageOrigin = "A6",
                    MessageType = "HC",
                    MessageDestination = "A4",
                    AppId = "37",
                    LocatorPrefix = ConfigurationManager.AppSettings["LocatorPrefix2"]
                },
                new MessageProps()
                {
                    TransOrigin = "A5",
                    MessageOrigin = "A5",
                    MessageType = "UG",
                    MessageDestination = "XX",
                    AppId = "02",
                    LocatorPrefix = ConfigurationManager.AppSettings["LocatorPrefix3"]
                },
                new MessageProps()
                {
                    TransOrigin = "A5",
                    MessageOrigin = "XX",
                    MessageType = "CG",
                    MessageDestination = "A5",
                    AppId = "02",
                    LocatorPrefix = ConfigurationManager.AppSettings["LocatorPrefix4"]
                }
            };

            _randomProps = new Random();
            _randomOps = new Random();

            var messageProp = _messageProps[_randomProps.Next(0, _messageProps.Count)];

            int operationNumber = _randomOps.Next(1, 4);

            Write(dbContext, i, messageProp.TransOrigin, messageProp.MessageOrigin, messageProp.MessageDestination, messageProp.MessageDestination, reqBytes, responseBytes);
        }

        public static void Write(DynamoDBContext dbContext, int i, string transOrigin, string messageOrigin, string messageDest, string msgType,
                                 byte[] reqBytes, byte[] respBytes)
        {

            try
            {
                var transLog = new TransactionLog()
                {
                    Id = Guid.NewGuid().ToString("N"),
                    AppID = 37.ToString(),
                    IsInbound = true,
                    LogDate = DateTime.Now.Date.ToString(CultureInfo.InvariantCulture),
                    MsgDestination = messageDest,
                    MsgOrigin = messageOrigin,
                    TransOrigin = transOrigin,
                    MsgLocator = GetLocator(),
                    MsgType = msgType,
                    RequestMessage = reqBytes,
                    ResponseMessage = respBytes,
                    LogTime = DateTime.Now.ToLocalTime(),
                    SentTime = DateTime.Now.ToLocalTime()
                };
                dbContext.Save(transLog);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }


        //private static void WriteToTransLog(DynamoDBContext dbContext, int i)
        //{
        //    try
        //    {
        //        var transLog = new TransactionLog()
        //        {
        //            Id = $"{i}",
        //            AppID = 37.ToString(),
        //            IsInbound = true,
        //            LogDate = DateTime.Now.Date.ToString(CultureInfo.InvariantCulture),
        //            MsgDestination = "XX",
        //            MsgOrigin = "A4",
        //            TransOrigin = "A4",
        //            MsgLocator = GetLocator(),
        //            MsgType = i % 2 == 0 ? "UG" : "UA",
        //            RequestMessage = Encoding.ASCII.GetBytes("Request Message ........")
        //        };
        //        dbContext.Save(transLog);
        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine(e);
        //        //throw;
        //    }
        //}

        private static string GetLocator()
        {
            string locator = DateTime.Now.Date.ToLongDateString() +":" + DateTime.Now.ToLongTimeString() + ":" +
                             new Random().NextDouble().ToString("N");

            return locator;
        }

        private static void Delete(DynamoDBContext dbContext)
        {
            var results = dbContext.Query<TransactionLog>("A5", QueryOperator.BeginsWith,
                                        "Thursday, August 10, 2017");
            Console.WriteLine($"Count = {results?.Count()}; hit enter to continue");
            foreach (var item in results)
            {
                dbContext.Delete(item);
            }
            Console.WriteLine($"Deleted all items = {results?.Count()}; hit enter to continue");

        }

        private static void ReadFromTransLogUsingPrimaryIndex(DynamoDBContext dbContext)
        {
            try
            {
                DateTime time = DateTime.Now;

                var results = dbContext.Query<TransactionLog>("A4", QueryOperator.BeginsWith, 
                                        DateTime.Now.Date.ToLongDateString());

                DateTime time2 = DateTime.Now;

                Console.WriteLine($"duration of query = {time2 - time}");

                Console.WriteLine($"Count = {results?.Count()}; hit enter to continue");

                foreach (var item in results)
                {
                    Console.WriteLine(
                        $"AppId = {item.AppID}, MsgDestination = {item.MsgDestination}, TransOrigin = {item.TransOrigin}, MsgLocator = {item.MsgLocator}, LogDate = {item.LogDate}, LogTime = {item.LogTime}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private static void ReadFromTransLogUsingGlobalSecondaryIndex(DynamoDBContext dbContext)
        {
            try
            {
                //QueryRequest queryRequest = new QueryRequest()
                //{
                //    TableName = transLogTableName,
                //    IndexName = "AppID-MsgOrigin-index",
                //    KeyConditionExpression = "TransOrigin = :v_TrO and MsgType = :v_MT",
                //    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                //    {
                //        { "v_TrO", new AttributeValue() {S = "A4" }},
                //        {"MsgType", new AttributeValue() {S = "UA"}}
                //    },
                //    ConsistentRead = true
                //};
                DateTime time = DateTime.Now;

                var results = dbContext.Query<TransactionLog>("37", new DynamoDBOperationConfig
                {
                    IndexName = "AppID-MsgOrigin-index"
                });

                DateTime time2 = DateTime.Now;

                Console.WriteLine($"duration of query = {time2 - time}");

                Console.WriteLine($"Count = {results?.Count()}");

                foreach (var item in results)
                {
                    Console.WriteLine(
                        $"AppId = {item.AppID}, MsgDestination = {item.MsgDestination}, TransOrigin = {item.TransOrigin}, MsgLocator = {item.MsgLocator}, LogDate = {item.LogDate}, LogTime = {item.LogTime}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
        private static void ReadFromTransLogUsingLocalSecondaryIndex(DynamoDBContext dbContext)
        {
            try
            {
                DateTime time = DateTime.Now;

                var results = dbContext.Query<TransactionLog>("A4", QueryOperator.Equal, new[] { "UA" }, new DynamoDBOperationConfig
                {
                    IndexName = "TransOrigin-MsgType-index"
                });

                DateTime time2 = DateTime.Now;

                Console.WriteLine($"duration of query = {time2 - time}");

                Console.WriteLine($"Count = {results?.Count()}");

                foreach (var item in results)
                {
                    Console.WriteLine(
                        $"AppId = {item.AppID}, MsgType: {item.MsgType}, MsgDestination = {item.MsgDestination}, TransOrigin = {item.TransOrigin}, MsgLocator = {item.MsgLocator}, LogDate = {item.LogDate}, LogTime = {item.LogTime}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private static void WaitUntilTableReady(AmazonDynamoDBClient client, string tableName)
        {
            string status = null;
            // Let us wait until table is created. Call DescribeTable.
            do
            {
                System.Threading.Thread.Sleep(5000); // Wait 5 seconds.
                try
                {
                    var res = client.DescribeTable(new DescribeTableRequest
                    {
                        TableName = tableName
                    });

                    Console.WriteLine("Table name: {0}, status: {1}",
                              res.Table.TableName,
                              res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }
                catch (ResourceNotFoundException exception)
                {
                    Console.WriteLine(exception);
                    // DescribeTable is eventually consistent. So you might
                    // get resource not found. So we handle the potential exception.
                }
            } while (status != "ACTIVE");
        }
    }
}
