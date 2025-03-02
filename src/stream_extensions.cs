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

    public static async Task HandleApiKey(this NetworkStream stream, int api_key) {

        if(api_key==18) {
            await stream.WriteInt8BigEndianAsync(2); // api_key array length 1 byte
            await stream.WriteInt16BigEndianAsync(18); // api_key 2 bytes
            await stream.WriteInt16BigEndianAsync(4); // min_version 2 bytes
            await stream.WriteInt16BigEndianAsync(4); // max_version 2 bytes
            await stream.WriteInt8BigEndianAsync(1); // tagBuffer 1 byte
            await stream.WriteInt32BigEndianAsync(500); // throttleTimeMs 4 bytes
            await stream.WriteInt8BigEndianAsync(1); // tagBuffer 1 byte
        }


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
    public static async Task ParseHeader(this NetworkStream stream) {
        byte[] binaryData = new byte[1024];
        
        int bytesRead = await stream.ReadAsync(binaryData, 0, binaryData.Length);    
        
        int message_length = BinaryPrimitives.ReadInt32BigEndian(binaryData[0..4]);
        int api_key = BinaryPrimitives.ReadInt16BigEndian(binaryData[4..6]);
        int api_version = BinaryPrimitives.ReadInt16BigEndian(binaryData[6..8]);
        int correlation_id = BinaryPrimitives.ReadInt32BigEndian(binaryData[8..12]);



        Console.WriteLine($"Message Length: {message_length}");
        Console.WriteLine($"Correlation Id: {correlation_id}");
        Console.WriteLine($"API Key: {api_key}");
        Console.WriteLine($"API Version: {api_version}");
        // Console.WriteLine($"API Version Error Code: {CheckApiVersion(api_version)}");
        await stream.WriteInt32BigEndianAsync(message_length); // 4 bytes
        await stream.WriteInt32BigEndianAsync(correlation_id); // 4 bytes
        await stream.WriteInt16BigEndianAsync(CheckApiVersion(api_version)); // error_code 2 bytes

        await stream.HandleApiKey(api_key);

    }
}