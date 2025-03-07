using System.Net.Sockets;
using System.Threading.Tasks;
using System.Buffers.Binary;
using System.Net;
using System.Text;

namespace App.Kafka;
public static class StreamExtensions
{
    public class ApiKey {
        public short api_key;
        public short min_version;
        public short max_version;
    }

    public static async Task WriteInt32BigEndianAsync(this NetworkStream stream, int input)
    {
        byte[] bytes = BitConverter.GetBytes(input);
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(bytes);
        }
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
    public static async Task WriteInt8BigEndianAsync(this NetworkStream stream, byte input) 
    {
        byte[] bytes =  new byte[] {input};
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



    public static async Task HandleApiVersionRequest(this NetworkStream stream, int correlation_id, int api_key, int api_version) {
        // read request body
        // 1 byte - Assuming length < 256 (0xff)

        byte[] client_software_name_length_bytes = new byte[1]; 
        _ = await stream.ReadAsync(client_software_name_length_bytes, 0, 1);
        byte client_software_name_length = (byte)(client_software_name_length_bytes[0]-1);

        byte[] client_software_name_bytes = new byte[client_software_name_length];
        _ = await stream.ReadAsync(client_software_name_bytes,0,client_software_name_length);
        string client_software_name = Encoding.UTF8.GetString(client_software_name_bytes);
        

        byte[] client_software_ver_length_bytes = new byte[1]; 
        _ = await stream.ReadAsync(client_software_ver_length_bytes, 0, 1);
        byte client_software_ver_length = (byte)(client_software_ver_length_bytes[0]-1);

        byte[] client_software_ver_bytes = new byte[client_software_ver_length];
        _ = await stream.ReadAsync(client_software_ver_bytes,0,client_software_ver_length);
        string client_software_ver = Encoding.UTF8.GetString(client_software_ver_bytes);

        Console.WriteLine("----------------------------------------");
        Console.WriteLine("ApiVersion[v4] Request Body");
        Console.WriteLine($"Client Software Length: {client_software_name_length} | Client Software Name: {client_software_name} | Client Software Version: {client_software_ver}");
        // response to handle ApiVersion request
        List<ApiKey> keys = new List<ApiKey>
        {   new ApiKey {api_key = 18, min_version = 4, max_version = 4},
            new ApiKey {api_key = 75, min_version = 0, max_version = 0}
        };

        int keys_length_bytes = 6; // 2 bytes for api_key + 2 bytes for min_version + 2 bytes for max_version
        byte api_key_array_length = (byte)(keys.Count);
        // 4 bytes response_size (not considered in defining response length)
        // 4 bytes for correlation_id + 2 bytes for error_code + 1 byte for api_key array length 
        // + (api_key_array_length * keys_length_bytes) + api_keys_array_length * 1 byte for tagBuffer 
        // + 4 bytes for throttleTimeMs + 1 byte for tagBuffer
        int response_size = 4 + 2 + 1 + (api_key_array_length * keys_length_bytes)+(api_key_array_length * 1) + 4 + 1;

        await stream.WriteInt32BigEndianAsync(response_size); // 4 bytes - response size excluding the size field itself
        await stream.WriteInt32BigEndianAsync(correlation_id); // correlation_id 4 bytes
        await stream.WriteInt16BigEndianAsync(CheckApiVersion(api_version)); // error_code 2 bytes
        await stream.WriteInt8BigEndianAsync((byte)(api_key_array_length+1)); // api_key array length 1 byte
        foreach(ApiKey key in keys) {
            await stream.WriteInt16BigEndianAsync(key.api_key); // api_key 2 bytes
            await stream.WriteInt16BigEndianAsync(key.min_version); // min_version 2 bytes
            await stream.WriteInt16BigEndianAsync(key.max_version); // max_version 2 bytes
            await stream.WriteInt8BigEndianAsync(0); // tagBuffer 1 byte
        }
        await stream.WriteInt32BigEndianAsync(0); // throttleTimeMs 4 bytes
        await stream.WriteInt8BigEndianAsync(0); // tagBuffer 1 byte
    }

    public static async Task HandleDescribeTopicPartitionRequest(this NetworkStream stream, int correlation_id) {
            // parse DescribeTopicPartition Request Body (v0)
            // topics array - COMPACT_ARRAY
                // topics_array_length - VARINT[actual array length + 1] - 1 byte 
                    // foreach topic in topics array
                        // topic_name_length - COMPACT_STRING -> Content_length - 1 byte
                        // topic_name - (topic_name_length - 1) bytes
                        // Tag_buffer - 1 byte
            // response_partition_limit - INT32 - 4 bytes
            // cursor - 1 byte
            // tag_buffer - 1 byte

            // topics_array_length - 1 bytes
            byte[] topics_array_length_bytes = new byte[1];
            _ = await stream.ReadAsync(topics_array_length_bytes,0,1);
            byte topics_array_length = (byte)(topics_array_length_bytes[0]-1);

            HashSet<string> topics = new HashSet<string>();

            for(int i = 0; i < topics_array_length; i++) {
                byte[] topics_name_length_bytes = new byte[1];
                _ = await stream.ReadAsync(topics_name_length_bytes,0,1);
                byte topics_name_length = (byte)(topics_name_length_bytes[0]-1);

                byte[] topics_name_bytes = new byte[topics_name_length];
                _ = await stream.ReadAsync(topics_name_bytes,0,topics_name_length);
                string topic_name = Encoding.UTF8.GetString(topics_name_bytes);

                topics.Add(topic_name);

                byte[] tag_buffer = new byte[1];
                _ = await stream.ReadAsync(tag_buffer,0,1);
            }

            byte[] response_partition_limit_bytes = new byte[4];
            _ = await stream.ReadAsync(response_partition_limit_bytes,0,4);
            int response_partition_limit = BinaryPrimitives.ReadInt32BigEndian(response_partition_limit_bytes);  
            
/*  
response header - 9 bytes
    message_length - 4 bytes
    correlation_id - 4 bytes
    tag_buffer - 1 byte
response body - 
    throttle_time_ms - 4 bytes
    topics_array
    topics array length - 1 byte - assumption: length < 256 (0xff [1 byte])
        foreach topic in topics array
            error_code - 2 bytes
            topic_name
                topic_name_length - 1 byte - assumption: length < 256 (0xff [1 byte])
                topic_name - (topic_name_length - 1) bytes
            topic_id - UUID - 16 bytes
            is_internal - 1 byte
            partitions_array
                array_length - 1 byte - set to 0 + 1 [consider empty array]
            topic_authorized_operations - 4 byte
            tag_buffer - 1 byte
    next_cursor array
    next_cursor array length - 1 byte assumption: length < 256 (0xff [1 byte])
        topic_name - COMPACT_STRING
            topic_name_length - 1 byte assumption: length < 256 (0xff [1 byte])
            topic_name - topic_name_length-1 bytes
        partition_index - INT32 - 4 bytes
    tag_buffer - 1 byte */

                int totalTopicsLengthBytes = 0;
                foreach(string topic in topics) {
                    totalTopicsLengthBytes += topic.Length;
                }
                
                int response_size = 9 + 4 + 1 + topics_array_length * (2 + 1 + 16 + 1 + 1 + 4 + 1) + totalTopicsLengthBytes;

                await stream.WriteInt32BigEndianAsync(response_size);
                await stream.WriteInt32BigEndianAsync(correlation_id);
                await stream.WriteInt8BigEndianAsync(0); // tag_buffer
                await stream.WriteInt32BigEndianAsync(0); // throttle_time_ms
                await stream.WriteInt8BigEndianAsync((byte)(topics_array_length+1)); // actual_length + 1
                foreach(string topic in topics) {
                    await stream.WriteInt16BigEndianAsync(3); // error_code - UNKNOWN_TOPIC
                    // topic_name
                        await stream.WriteInt8BigEndianAsync((byte)(topic.Length + 1)); // topic_name_length
                        byte[] topic_name_bytes = Encoding.UTF8.GetBytes(topic);
                        await stream.WriteAsync(topic_name_bytes); 
                    // topic_id
                    byte[] topic_id = new byte[16];
                    Span<byte> byteSpan = topic_id.AsSpan();
                    byteSpan.Fill(0); // Set all elements to 0 - UUID - 00000000-0000-0000-0000-000000000000 [without the dashes]
                    await stream.WriteAsync(topic_id);
                    await stream.WriteInt8BigEndianAsync(0); // is_internal
                    await stream.WriteInt8BigEndianAsync(1); // partitions_array_length - length + 1 
                    // topic_authorized_operations
                    int topic_authorized_operations = 0x00000df8; 
                    await stream.WriteInt32BigEndianAsync(topic_authorized_operations);
                    await stream.WriteInt8BigEndianAsync(0); // tag_buffer
                }
                await stream.WriteInt8BigEndianAsync(0xff); // next_cursor_array_length 1 byte
                // next_cursor_array
                    //string next_cursor_topic_name = "Test Topic";
                    // await stream.WriteInt8BigEndianAsync((byte)(11)); // topic_name_length - 11 - 1 byte
                    // byte[] next_cursor_topic_name_bytes = Encoding.UTF8.GetBytes(next_cursor_topic_name);
                    // await stream.WriteAsync(next_cursor_topic_name_bytes); // topic_name - 10 bytes
                // await stream.WriteInt8BigEndianAsync(0); // tag_buffer
    }

    public static async Task ParseRequest(this NetworkStream stream) {
        // byte[] binaryData = new byte[1024];
        
        // int bytesRead = await stream.ReadAsync(binaryData, 0, binaryData.Length);    
        
        // message_length - 4 bytes
        byte[] message_length_bytes = new byte[4];
        _ = await stream.ReadAsync(message_length_bytes, 0, 4);
        int message_length = BinaryPrimitives.ReadInt32BigEndian(message_length_bytes);
    


        // api_key - 2 bytes
        byte[] api_key_bytes = new byte[2];
        _ = await stream.ReadAsync(api_key_bytes, 0, 2);
        short api_key = BinaryPrimitives.ReadInt16BigEndian(api_key_bytes);

        // api_version - 2 bytes
        byte[] api_version_bytes = new byte[2];
        _ = await stream.ReadAsync(api_version_bytes, 0, 2);
        short api_version = BinaryPrimitives.ReadInt16BigEndian(api_version_bytes);

        // correlation_id - 4 bytes
        byte[] correlation_id_bytes = new byte[4];
        _ = await stream.ReadAsync(correlation_id_bytes,0,4);
        int correlation_id = BinaryPrimitives.ReadInt32BigEndian(correlation_id_bytes);

        // client_id_length - 2 bytes
        byte[] client_id_length_bytes = new byte[2];
        _ = await stream.ReadAsync(client_id_length_bytes, 0, 2);
        short client_id_length = BinaryPrimitives.ReadInt16BigEndian(client_id_length_bytes);

        // client_id - NULLABLE_STRING - client_id_length bytes
        byte[] client_id_bytes = new byte[client_id_length];
        _ = await stream.ReadAsync(client_id_bytes, 0, client_id_length);
        string client_id = Encoding.UTF8.GetString(client_id_bytes);

        Console.WriteLine("Request Header");
        Console.WriteLine($"Message Length: {message_length} | Correlation Id: {correlation_id} | API Key: {api_key} | API Version: {api_version} | Client Id: {client_id}");
        
        // read tag_buffer in header
        byte[] tag_buffer = new byte[1];
        _ = await stream.ReadAsync(tag_buffer,0,1);

        // // Console.WriteLine($"API Version Error Code: {CheckApiVersion(api_version)}");
        // // 23 bytes - 4 bytes message_length + 4 bytes correlation_id + 2 bytes error_code + 13 bytes APIVersion Response
        // // await stream.WriteInt32BigEndianAsync(19); // 4 bytes - response size excluding the size field itself
        // // await stream.WriteInt32BigEndianAsync(correlation_id); // 4 bytes
        // // await stream.WriteInt16BigEndianAsync(CheckApiVersion(api_version)); // error_code 2 bytes
        switch(api_key) {
            // api_key == 18 -> ApiVersion Request
            case 18: await stream.HandleApiVersionRequest(correlation_id, api_key, api_version);
                break;
            // api_key == 75 -> DescribeTopicPartitionRequest
            case 75: await stream.HandleDescribeTopicPartitionRequest(correlation_id);
                break;
            default: return;
        }
        
    }
}