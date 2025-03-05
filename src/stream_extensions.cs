using System.Net.Sockets;
using System.Threading.Tasks;
using System.Buffers.Binary;
using System.Net;

namespace App.Kafka;
public static class StreamExtensions
{

    public static async Task WriteInt32BigEndianAsync(this NetworkStream stream, int input)
    {
        byte[] bytes = BitConverter.GetBytes(input);
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(bytes);
        }
        await stream.WriteAsync(bytes);
    }

    public class ApiKey {
        public short api_key;
        public short min_version;
        public short max_version;
    }


    public static async Task HandleApiVersionRequest(this NetworkStream stream, int correlation_id, int api_key, int api_version) {
        // response to handle ApiVersion request
        List<ApiKey> keys = new List<ApiKey>
        {   new ApiKey {api_key = 18, min_version = 4, max_version = 4},
            new ApiKey {api_key = 75, min_version = 0, max_version = 0}
        };

        int keys_length_bytes = 6; // 2 bytes for api_key + 2 bytes for min_version + 2 bytes for max_version
        byte api_key_array_length = (byte)(keys.Count + 1);
        // 4 bytes response_size (not considered in defining response length)
        // 4 bytes for correlation_id + 2 bytes for error_code + 1 byte for api_key array length 
        // + (api_key_array_length * keys_length_bytes) + 1 byte for tagBuffer 
        // + 4 bytes for throttleTimeMs + 1 byte for tagBuffer
        int response_size = 4 + 2 + 1 + (api_key_array_length * keys_length_bytes) + 1 + 4 + 1;

        await stream.WriteInt32BigEndianAsync(response_size); // 4 bytes - response size excluding the size field itself
        await stream.WriteInt32BigEndianAsync(correlation_id); // correlation_id 4 bytes
        await stream.WriteInt16BigEndianAsync(CheckApiVersion(api_version)); // error_code 2 bytes
        await stream.WriteInt8BigEndianAsync(api_key_array_length); // api_key array length 1 byte
        foreach(ApiKey key in keys) {
            await stream.WriteInt16BigEndianAsync(key.api_key); // api_key 2 bytes
            await stream.WriteInt16BigEndianAsync(key.min_version); // min_version 2 bytes
            await stream.WriteInt16BigEndianAsync(key.max_version); // max_version 2 bytes
        }
        await stream.WriteInt8BigEndianAsync(0); // tagBuffer 1 byte
        await stream.WriteInt32BigEndianAsync(0); // throttleTimeMs 4 bytes
        await stream.WriteInt8BigEndianAsync(0); // tagBuffer 1 byte
        // if(api_key==18) {
        //     await stream.WriteInt8BigEndianAsync(2); // api_key array length + 1 -  1 byte
        //     foreach(ApiKey key in keys) {
        //         await stream.WriteInt16BigEndianAsync(key.api_key); // api_key 2 bytes
        //         await stream.WriteInt16BigEndianAsync(key.min_version); // min_version 2 bytes
        //         await stream.WriteInt16BigEndianAsync(key.max_version); // max_version 2 bytes
        //     }
        //     await stream.WriteInt8BigEndianAsync(0); // tagBuffer 1 byte
        //     await stream.WriteInt32BigEndianAsync(0); // throttleTimeMs 4 bytes
        //     await stream.WriteInt8BigEndianAsync(0); // tagBuffer 1 byte
        // }
        // // response to handle DescribeTopicPartitions API request
        // if(api_key==75) {
        //     await stream.WriteInt8BigEndianAsync(2); // api_key array length 1 byte
        //     await stream.WriteInt16BigEndianAsync(75); // api_key 2 bytes
        //     await stream.WriteInt16BigEndianAsync(0); // min_version 2 bytes
        //     await stream.WriteInt16BigEndianAsync(0); // max_version 2 bytes
        //     await stream.WriteInt8BigEndianAsync(0); // tagBuffer 1 byte
        //     await stream.WriteInt32BigEndianAsync(0); // throttleTimeMs 4 bytes
        //     await stream.WriteInt8BigEndianAsync(0); // tagBuffer 1 byte
        // }

    }

    public static async Task HandleDescribeTopicPartition(this NetworkStream stream, int api_key) {
            // topics array length - 1 byte
                // length(name) + 1 - varint 2 bytes
                // name - length(name) bytes
            // tag_buffer - 1 byte
            // response_partition_limit - INT32 - 4 bytes
            // topic_name array length - 1 byte
                // length(topic_name) + 1 - varint 2 bytes
                // topic_name - length(name) bytes
                // partition_index - INT32 - 4 bytes
            // tag_buffer - 1 byte
            await Task.Delay(0);
    }

    public static async Task WriteInt8BigEndianAsync(this NetworkStream stream, byte input) 
    {
        byte[] bytes =  new byte[] {input};
        await stream.WriteAsync(bytes);
    }


    public static async Task WriteInt16BigEndianAsync(this NetworkStream stream, short input)
    {
        byte[] bytes = BitConverter.GetBytes(input);
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(bytes);
        }
        await stream.WriteAsync(bytes);
    }
    public static short CheckApiVersion(int api_version)
    {
        short error_code = 0;
        if (api_version < 0 || api_version > 4)
        {
            error_code=35;
        }
        
        return error_code;
    }
    public static async Task ParseRequest(this NetworkStream stream) {
        byte[] binaryData = new byte[1024];
        
        int bytesRead = await stream.ReadAsync(binaryData, 0, binaryData.Length);    
        
        int message_length = BinaryPrimitives.ReadInt32BigEndian(binaryData[0..4]);
        short api_key = BinaryPrimitives.ReadInt16BigEndian(binaryData[4..6]);
        short api_version = BinaryPrimitives.ReadInt16BigEndian(binaryData[6..8]);
        int correlation_id = BinaryPrimitives.ReadInt32BigEndian(binaryData[8..12]);

        short client_id_length = BinaryPrimitives.ReadInt16BigEndian(binaryData[12..14]);
        string client_id = System.Text.Encoding.UTF8.GetString(binaryData[14..(14+client_id_length)]);


        Console.WriteLine($"Message Length: {message_length}");
        Console.WriteLine($"Correlation Id: {correlation_id}");
        Console.WriteLine($"API Key: {api_key}");
        Console.WriteLine($"API Version: {api_version}");
        Console.WriteLine($"Client Id: {client_id}");

        // Console.WriteLine($"API Version Error Code: {CheckApiVersion(api_version)}");
        // 23 bytes - 4 bytes message_length + 4 bytes correlation_id + 2 bytes error_code + 13 bytes APIVersion Response
        // await stream.WriteInt32BigEndianAsync(19); // 4 bytes - response size excluding the size field itself
        // await stream.WriteInt32BigEndianAsync(correlation_id); // 4 bytes
        // await stream.WriteInt16BigEndianAsync(CheckApiVersion(api_version)); // error_code 2 bytes

        await stream.HandleApiVersionRequest(correlation_id, api_key, api_version);
    }

    async public static Task ParseApiVersionRequest(this NetworkStream stream)
    {
        await stream.ParseRequest();
        await stream.FlushAsync();
    }
}