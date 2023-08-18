#if FAST_KV_PROFILE && DEBUG
#define FAST_KV_PROFILE
#else
#undef FAST_KV_PROFILE
#endif

using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Runtime.InteropServices;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Buffers;
using System.Linq;
using Debug = UnityEngine.Debug;

namespace xiabob.FastKV
{

    internal static class BytesExt
    {
        public static bool IsSequenceEqual(this byte[] a, byte[] b)
        {
            if (a == null || b == null) return false;
            if (a.Length != b.Length) return false;
            return a.SequenceEqual(b);
        }
    }

    internal static class MemoryMappedViewAccessorExt
    {
        // https://stackoverflow.com/questions/7956167/how-can-i-quickly-read-bytes-from-a-memory-mapped-file-in-net
        // https://learn.microsoft.com/en-us/dotnet/api/system.runtime.interopservices.safebuffer.acquirepointer#system-runtime-interopservices-safebuffer-acquirepointer(system-byte*@)
        public static unsafe void ReadBytesFastButUnsafe(this MemoryMappedViewAccessor accessor, int position, byte[] arr, int length)
        {
            try
            {
                byte* ptr = (byte*)0;
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                Marshal.Copy(IntPtr.Add(new IntPtr(ptr), position), arr, 0, length);
            }
            catch (Exception e)
            {
                accessor.ReadArray<byte>(position, arr, 0, length);
                Debug.LogError($"Read bytes error: {e.Message}");
            }
            finally
            {
                accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }
        public static unsafe void WriteBytesFastButUnsafe(this MemoryMappedViewAccessor accessor, int position, byte[] data)
        {
            byte* ptr = (byte*)0;
            try
            {
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                Marshal.Copy(data, 0, IntPtr.Add(new IntPtr(ptr), position), data.Length);
            }
            catch
            {
                accessor.WriteArray<byte>(position, data, 0, data.Length);
            }
            finally
            {
                accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }
    }


    internal enum HeadersFlag
    {
        Empty = 0,
        Nomral,
    }

    internal class Headers
    {
        public byte Version;
        public HeadersFlag Flag;
        public byte[] HeadersCheckHash;
        public ushort HeaderCount;
        public List<KeyValueHeader> KeyValueHeaders;

        public int HeaderCountPosition { get; set; }
        public int HeaderActualSize { get; set; }
        public KeyValueHeader GetLastKeyValueHeader()
        {
            if (HeaderCount > 0) return KeyValueHeaders[HeaderCount - 1];
            else return null;
        }
    }

    internal class KeyValueHeader
    {
        public KeyValueFlag Flag;
        public byte KeySize;
        public string Key;
        public int BodySize;
        public int BodyOffset;

        public int HeaderOffset { set; get; }
        public int HeaderSize { private set; get; }

        public int CalculateHeaderSize() => HeaderSize = 1 + Marshal.SizeOf(KeySize) + KeySize + Marshal.SizeOf(BodySize) + Marshal.SizeOf(BodyOffset);

        public int FullWriteToMemoryMappedFile(MemoryMappedViewAccessor accessor, int position)
        {
            HeaderOffset = position;

            accessor.Write(position, (byte)Flag);
            position += 1;

            accessor.Write(position, KeySize);
            position += Marshal.SizeOf(KeySize);

            accessor.WriteBytesFastButUnsafe(position, EncodeKey(Key));
            position += KeySize;

            accessor.Write(position, BodySize);
            position += Marshal.SizeOf(BodySize);

            accessor.Write(position, BodyOffset);
            position += Marshal.SizeOf(BodyOffset);

            return position;
        }

        public int FullReadFromMemoryMappedFile(MemoryMappedViewAccessor accessor, int position)
        {
            HeaderOffset = position;

            Flag = (KeyValueFlag)accessor.ReadByte(position);
            position += 1;

            KeySize = accessor.ReadByte(position);
            position += Marshal.SizeOf(KeySize);

            var keyBytes = new byte[KeySize];
            accessor.ReadBytesFastButUnsafe(position, keyBytes, KeySize);
            Key = DecodeKey(keyBytes);
            position += KeySize;

            BodySize = accessor.ReadInt32(position);
            position += Marshal.SizeOf(BodySize);

            BodyOffset = accessor.ReadInt32(position);
            position += Marshal.SizeOf(BodyOffset);

            CalculateHeaderSize();
            return position;
        }

        public void UpdateAndWriteKeyValueFlag(MemoryMappedViewAccessor accessor, KeyValueFlag flag)
        {
            Flag = flag;
            accessor.Write(HeaderOffset, (byte)flag);
        }

        private byte[] EncodeKey(string key)
        {
            var bytes = Encoding.UTF8.GetBytes(key);
            for (int i = 0; i < bytes.Length; i++)
            {
                bytes[i] ^= 0xff;
            }
            return bytes;
        }
        private string DecodeKey(byte[] key)
        {
            for (int i = 0; i < key.Length; i++)
            {
                key[i] ^= 0xff;
            }
            return Encoding.UTF8.GetString(key);
        }
    }

    internal enum KeyValueFlag
    {
        Normal = 0,
        Delete,
        Null,
    }

    internal class Contents
    {
        public List<KeyValueBody> Bodys = new List<KeyValueBody>();
        public int ContentsActualSize { private set; get; }

        private Dictionary<string, KeyValueBody> m_KeyValueCache = new Dictionary<string, KeyValueBody>();

        public void AddContentData(string key, KeyValueBody body)
        {
            Bodys.Add(body);
            if (m_KeyValueCache.TryGetValue(key, out var cachedData))
            {
                cachedData.Header.Flag = KeyValueFlag.Delete;
            }
            m_KeyValueCache[key] = body;
            ContentsActualSize = body.Header.BodyOffset + body.Header.BodySize;
        }

        public void Reset()
        {
            Bodys.Clear();
            m_KeyValueCache.Clear();
            ContentsActualSize = 0;
        }

        public void DeleteContentData(string key, KeyValueHeader header)
        {
            var data = Bodys.Find(x => x.Header == header);
            if (data == null) return;

            Bodys.Remove(data);
            if (m_KeyValueCache.TryGetValue(key, out var cachedData) && cachedData == data)
            {
                m_KeyValueCache.Remove(key);
            }
            ContentsActualSize -= header.BodySize;
        }

        public bool TryGetContentData(string key, out KeyValueBody data)
        {
            return m_KeyValueCache.TryGetValue(key, out data) && data.Header.Flag != KeyValueFlag.Delete;
        }
    }

    internal class KeyValueBody
    {
        public byte[] Value;
        public byte[] Checksum;
        public KeyValueHeader Header { set; get; }
        public FastKV.IHashAlgorithm HashAlgorithm { private set; get; }
        public int ChecksumSize => HashAlgorithm.HashSize / 8;
        public int ValueSize => Header.BodySize - ChecksumSize;
        public bool DidReadValue => Value != null;

        public KeyValueBody(FastKV.IHashAlgorithm hashAlgorithm, KeyValueHeader header)
        {
            HashAlgorithm = hashAlgorithm;
            Header = header;
        }

        public void FullWriteBack(MemoryMappedViewAccessor accessor)
        {
            accessor.WriteBytesFastButUnsafe((int)Header.BodyOffset, Value);
            accessor.WriteBytesFastButUnsafe((int)Header.BodyOffset + Value.Length, Checksum);
        }

        public void WriteValue(MemoryMappedViewAccessor accessor, byte[] value)
        {
            Value = value;
            Checksum = HashAlgorithm.ComputeHash(value, 0, value.Length);
            accessor.WriteBytesFastButUnsafe((int)Header.BodyOffset, Value);
            accessor.WriteBytesFastButUnsafe((int)Header.BodyOffset + Value.Length, Checksum);
        }

        public void ReadValue(MemoryMappedViewAccessor accessor)
        {
            if (DidReadValue) return;

            Value = new byte[ValueSize];
            accessor.ReadBytesFastButUnsafe((int)Header.BodyOffset, Value, Value.Length);
            Checksum = new byte[ChecksumSize];
            accessor.ReadBytesFastButUnsafe((int)Header.BodyOffset + Value.Length, Checksum, Checksum.Length);
        }

        public bool CheckValueValid()
        {
            return HashAlgorithm.ComputeHash(Value, 0, Value.Length).IsSequenceEqual(Checksum);
        }
    }

    internal class Converter
    {
        public static bool ToBool(byte[] bytes) => BitConverter.ToBoolean(bytes, 0);
        public static int ToInt(byte[] bytes) => BitConverter.ToInt32(bytes, 0);
        public static long ToLong(byte[] bytes) => BitConverter.ToInt64(bytes, 0);
        public static float ToFloat(byte[] bytes) => BitConverter.ToSingle(bytes, 0);
        public static double ToDouble(byte[] bytes) => BitConverter.ToDouble(bytes, 0);
        public static string ToChars(byte[] bytes) => Encoding.UTF8.GetString(bytes);
        public static byte[] ToBytes(byte[] bytes)
        {
            var newBytes = new byte[bytes.Length];
            Buffer.BlockCopy(bytes, 0, newBytes, 0, newBytes.Length);
            return newBytes;
        }
    }

    public class FastKV
    {

        public interface IHashAlgorithm
        {
            public int HashSize { get; }
            public byte[] ComputeHash(byte[] buffer, int offset, int count);
        }

        class XORChecksumAlgorithm : IHashAlgorithm
        {
            private byte[] m_Buffer;
            public int HashSize => 8 * 1;

            public XORChecksumAlgorithm()
            {
                m_Buffer = new byte[HashSize / 8];
            }

            public byte[] ComputeHash(byte[] buffer, int offset, int count)
            {
                m_Buffer[0] = 0;

                int blockSize = 4;
                int loopCount = count / blockSize;
                int i = 0;
                for (int loop = 0; loop < loopCount; loop++)
                {
                    i = offset + loop * blockSize;
                    m_Buffer[0] ^= buffer[i];
                    m_Buffer[0] ^= buffer[i + 1];
                    m_Buffer[0] ^= buffer[i + 2];
                    m_Buffer[0] ^= buffer[i + 3];
                    i += blockSize;
                }
                for (; i < offset + count; i++)
                {
                    m_Buffer[0] ^= buffer[i];
                }
                return Converter.ToBytes(m_Buffer);
            }
        }

        private IHashAlgorithm m_HeaderHashAlgorithm;
        private IHashAlgorithm m_ValueHashAlgorithm;
        private byte[] m_EncryptionKey;
        private string m_HeaderFilePath, m_ContentFilePath;

        private Headers m_Headers;
        private MemoryMappedFile m_HeadersMMF;
        private MemoryMappedViewAccessor m_HeadersViewAccessor;
        private long m_HeadersMMFSize;

        private Contents m_Contents;
        private MemoryMappedFile m_ContentsMMF;
        private MemoryMappedViewAccessor m_ContentsViewAccessor;
        private long m_ContentsMMFSize;

        public static FastKV Open(string fileName, string encryptionKey, IHashAlgorithm headerHashAlgorithm = null, IHashAlgorithm valueHashAlgorithm = null)
        {
            return Open(fileName, Environment.SystemPageSize * 2, Environment.SystemPageSize * 25, encryptionKey, headerHashAlgorithm, valueHashAlgorithm);
        }

        public static FastKV Open(string fileName, int headerFileSize, int contentsFileSize, string encryptionKey, IHashAlgorithm headerHashAlgorithm = null, IHashAlgorithm valueHashAlgorithm = null)
        {
#if FAST_KV_PROFILE
            Debug.unityLogger.filterLogType = LogType.Log;
#endif
            var kv = new FastKV();
            kv.Init(fileName, headerFileSize, contentsFileSize, encryptionKey, headerHashAlgorithm, valueHashAlgorithm);
            return kv;
        }

        private FastKV()
        {
        }

        ~FastKV()
        {
            Close();
        }

        private void Init(string fileName, int headerFileSize, int contentsFileSize, string encryptionKey, IHashAlgorithm headerHashAlgorithm, IHashAlgorithm valueHashAlgorithm)
        {
            m_HeaderHashAlgorithm = headerHashAlgorithm ?? new XORChecksumAlgorithm();
            m_ValueHashAlgorithm = valueHashAlgorithm ?? new XORChecksumAlgorithm();
            SetupMemoryMapFiles(fileName, headerFileSize, contentsFileSize);
            SetupEncryptionKey(encryptionKey);
            LoadHeaders();
            LoadContents();
            CleanMemoryMappedFiles();
        }

        private void SetupMemoryMapFiles(string fileName, int headerFileSize, int contentsFileSize)
        {
            var folder = Path.Combine(Application.persistentDataPath, "FastKV");
            if (!Directory.Exists(folder)) Directory.CreateDirectory(folder);

            m_HeaderFilePath = Path.Combine(folder, $"{fileName}.meta");
            if (File.Exists(m_HeaderFilePath))
            {
                m_HeadersMMF = MemoryMappedFile.CreateFromFile(m_HeaderFilePath, FileMode.Open);
            }
            else
            {
                headerFileSize = Mathf.CeilToInt((headerFileSize * 1.0f / Environment.SystemPageSize)) * Environment.SystemPageSize;
                m_HeadersMMF = MemoryMappedFile.CreateFromFile(m_HeaderFilePath, FileMode.OpenOrCreate, null, headerFileSize);
            }
            m_HeadersViewAccessor = m_HeadersMMF.CreateViewAccessor();
            m_HeadersMMFSize = m_HeadersViewAccessor.Capacity;

            m_ContentFilePath = Path.Combine(folder, fileName);
            if (File.Exists(m_ContentFilePath))
            {
                m_ContentsMMF = MemoryMappedFile.CreateFromFile(m_ContentFilePath, FileMode.Open);
            }
            else
            {
                contentsFileSize = Mathf.CeilToInt((contentsFileSize * 1.0f / Environment.SystemPageSize)) * Environment.SystemPageSize;
                m_ContentsMMF = MemoryMappedFile.CreateFromFile(m_ContentFilePath, FileMode.OpenOrCreate, null, contentsFileSize);
            }
            m_ContentsViewAccessor = m_ContentsMMF.CreateViewAccessor();
            m_ContentsMMFSize = m_ContentsViewAccessor.Capacity;
        }

        private void SetupEncryptionKey(string encryptionKey)
        {
            if (string.IsNullOrEmpty(encryptionKey))
            {
                encryptionKey = "EncryptionKey";
            }
            m_EncryptionKey = Encoding.UTF8.GetBytes(encryptionKey);
        }

        private void LoadHeaders()
        {
#if FAST_KV_PROFILE
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
#endif
            LoadHeadersInternal();
#if FAST_KV_PROFILE
            stopwatch.Stop();
            Debug.Log($"[FastKV] Load headers time: {stopwatch.ElapsedTicks * 1f / TimeSpan.TicksPerMillisecond}");
#endif
        }
        private void LoadHeadersInternal()
        {
            m_Headers = new Headers();

            int position = 0;
            m_Headers.Version = m_HeadersViewAccessor.ReadByte(position);
            position += Marshal.SizeOf(m_Headers.Version);

            m_Headers.Flag = (HeadersFlag)m_HeadersViewAccessor.ReadByte(position);
            position += 1;

            bool checkChecksumSuccess = true;
            m_Headers.HeadersCheckHash = new byte[m_HeaderHashAlgorithm.HashSize / 8];
            if (m_Headers.Flag >= HeadersFlag.Nomral)
            {
                m_HeadersViewAccessor.ReadBytesFastButUnsafe(position, m_Headers.HeadersCheckHash, m_Headers.HeadersCheckHash.Length);
                if (!CheckHeaderChecksumHashValid())
                {
                    checkChecksumSuccess = false;
#if UNITY_INCLUDE_TESTS
                    Debug.LogWarning($"[FastKV] Invalid header checksum hash");
#else
                    Debug.LogError($"[FastKV] Invalid header checksum hash");
#endif
                }
            }
            position += m_Headers.HeadersCheckHash.Length;

            position += 128; // 预留128字节，用于后续增加字段

            m_Headers.HeaderCountPosition = position;
            if (checkChecksumSuccess && m_Headers.Flag >= HeadersFlag.Nomral)
            {
                m_Headers.HeaderCount = m_HeadersViewAccessor.ReadUInt16(position);
            }
            position += Marshal.SizeOf(m_Headers.HeaderCount);
            m_Headers.KeyValueHeaders = new List<KeyValueHeader>();
            for (int i = 0; i < m_Headers.HeaderCount; i++)
            {
                KeyValueHeader header = new KeyValueHeader();
                position = header.FullReadFromMemoryMappedFile(m_HeadersViewAccessor, position);
                m_Headers.KeyValueHeaders.Add(header);
            }

            bool needUpdateHeaderChecksumHash = false;
            if (!checkChecksumSuccess)
            {
                m_Headers.Flag = HeadersFlag.Empty;
                m_HeadersViewAccessor.Write(m_Headers.HeaderCountPosition, 0);
                needUpdateHeaderChecksumHash = true;
            }
            if (m_Headers.Flag < HeadersFlag.Nomral)
            {
                m_Headers.Flag = HeadersFlag.Nomral;
                m_HeadersViewAccessor.Write(Marshal.SizeOf(m_Headers.Version), (byte)m_Headers.Flag);
                needUpdateHeaderChecksumHash = true;
            }
            if (needUpdateHeaderChecksumHash) UpdateHeaderChecksumHash();

            m_Headers.HeaderActualSize = position;
        }

        private bool CheckHeaderChecksumHashValid() => CheckChecksumHashValid(CalculateHeaderChecksumHash(), m_Headers.HeadersCheckHash);
        private byte[] CalculateHeaderChecksumHash()
        {
            int position = Marshal.SizeOf(m_Headers.Version) + 1 + m_Headers.HeadersCheckHash.Length;
            if (m_HeadersViewAccessor.Capacity <= position) return null;

            int length = (int)m_HeadersViewAccessor.Capacity - position;
            byte[] buffer = ArrayPool<byte>.Shared.Rent(length);
            m_HeadersViewAccessor.ReadBytesFastButUnsafe(position, buffer, length);
            var result = m_HeaderHashAlgorithm.ComputeHash(buffer, 0, length);
            ArrayPool<byte>.Shared.Return(buffer);
            return result;
        }
        private void UpdateHeaderChecksumHash()
        {
            m_Headers.HeadersCheckHash = CalculateHeaderChecksumHash();
            m_HeadersViewAccessor.WriteBytesFastButUnsafe(Marshal.SizeOf(m_Headers.Version) + 1, m_Headers.HeadersCheckHash);
        }

        private bool CheckChecksumHashValid(byte[] calcultedHash, byte[] expectedHash) => calcultedHash.IsSequenceEqual(expectedHash);

        private void LoadContents()
        {
#if FAST_KV_PROFILE
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
#endif
            LoadContentsInternal();
#if FAST_KV_PROFILE
            stopwatch.Stop();
            Debug.Log($"[FastKV] Load contents time: {stopwatch.ElapsedTicks * 1f / TimeSpan.TicksPerMillisecond}");
#endif
        }
        private void LoadContentsInternal()
        {
            bool checkValueChecksumSuccess = true;
            m_Contents = new Contents();
            foreach (var header in m_Headers.KeyValueHeaders)
            {
                var keyValueBody = new KeyValueBody(m_ValueHashAlgorithm, header);
                if (header.Flag == KeyValueFlag.Delete)
                {
                    m_Contents.AddContentData(header.Key, keyValueBody);
                    continue;
                }

                keyValueBody.ReadValue(m_ContentsViewAccessor);
                if (keyValueBody.CheckValueValid())
                {
                    m_Contents.AddContentData(header.Key, keyValueBody);
                }
                else
                {
                    checkValueChecksumSuccess = false;
                    break;
                }
            }

            if (!checkValueChecksumSuccess)
            {
                m_Headers.HeaderCount = 0;
                m_Headers.KeyValueHeaders.Clear();
                m_HeadersViewAccessor.Write(m_Headers.HeaderCountPosition, m_Headers.HeaderCount);
                UpdateHeaderChecksumHash();

                m_Contents.Reset();
#if UNITY_INCLUDE_TESTS
                Debug.LogWarning($"[FastKV] Invalid value checksum hash");
#else
                Debug.LogError($"[FastKV] Invalid value checksum hash");
#endif
            }
        }

        private void CleanMemoryMappedFiles()
        {
#if FAST_KV_PROFILE
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
#endif
            CleanMemoryMappedFileInternal();

#if FAST_KV_PROFILE
            stopwatch.Stop();
            Debug.Log($"[FastKV] Clean memory mapped file time: {stopwatch.ElapsedTicks * 1f / TimeSpan.TicksPerMillisecond}");
#endif
        }
        private void CleanMemoryMappedFileInternal()
        {
            if (m_Headers.HeaderCount == 0) return;

            int removedContentOffset = 0;
            foreach (var keyValueHeader in m_Headers.KeyValueHeaders)
            {
                if (keyValueHeader.Flag == KeyValueFlag.Delete)
                {
                    m_Contents.DeleteContentData(keyValueHeader.Key, keyValueHeader);
                    removedContentOffset += keyValueHeader.BodySize;
                    continue;
                }
                if (removedContentOffset == 0)
                {
                    continue;
                }

                m_Contents.TryGetContentData(keyValueHeader.Key, out KeyValueBody keyValueBody);
                if (!keyValueBody.DidReadValue)
                {
                    keyValueBody.ReadValue(m_ContentsViewAccessor);
                }
                keyValueHeader.BodyOffset -= removedContentOffset;
                keyValueBody.FullWriteBack(m_ContentsViewAccessor);
            }
            if (removedContentOffset == 0) return;

            m_Headers.KeyValueHeaders.RemoveAll(x => x.Flag == KeyValueFlag.Delete);
            m_Headers.HeaderCount = (ushort)m_Headers.KeyValueHeaders.Count;

            int position = m_Headers.HeaderCountPosition;
            m_HeadersViewAccessor.Write(position, m_Headers.HeaderCount);
            position += Marshal.SizeOf(m_Headers.HeaderCount);
            for (int i = 0; i < m_Headers.HeaderCount; i++)
            {
                position = m_Headers.KeyValueHeaders[i].FullWriteToMemoryMappedFile(m_HeadersViewAccessor, position);
            }
            UpdateHeaderChecksumHash();

            m_Headers.HeaderActualSize = position;
        }

        private void ExpandHeaderMemoryMappedFile()
        {
#if FAST_KV_PROFILE
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
#endif
            ExpandHeaderMemoryMappedFileInternal();
#if FAST_KV_PROFILE
            stopwatch.Stop();
            Debug.Log($"[FastKV] Expand header memory mapped file time: {stopwatch.ElapsedTicks * 1f / TimeSpan.TicksPerMillisecond}");
#endif
        }
        private void ExpandHeaderMemoryMappedFileInternal()
        {
            m_HeadersViewAccessor.Flush();
            m_HeadersViewAccessor.Dispose();
            m_HeadersMMF.Dispose();

            // 对于header的mmf大小，直接大小x2即可
            m_HeadersMMFSize *= 2;
            m_HeadersMMF = MemoryMappedFile.CreateFromFile(m_HeaderFilePath, FileMode.Open, null, m_HeadersMMFSize);
            m_HeadersViewAccessor = m_HeadersMMF.CreateViewAccessor();
            UpdateHeaderChecksumHash();
        }

        private void ExpandContentsMemoryMappedFile(long expectContentsSize)
        {
#if FAST_KV_PROFILE
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
#endif
            ExpandContentsMemoryMappedFileInternal(expectContentsSize);
#if FAST_KV_PROFILE
            stopwatch.Stop();
            Debug.Log($"[FastKV] Expand contents memory mapped file time: {stopwatch.ElapsedTicks * 1f / TimeSpan.TicksPerMillisecond}");
#endif
        }
        private void ExpandContentsMemoryMappedFileInternal(long expectContentsSize)
        {
            m_ContentsViewAccessor.Flush();
            m_ContentsViewAccessor.Dispose();
            m_ContentsMMF.Dispose();

            m_ContentsMMFSize = (expectContentsSize / m_ContentsMMFSize + 1) * m_ContentsMMFSize;
            m_ContentsMMF = MemoryMappedFile.CreateFromFile(m_ContentFilePath, FileMode.OpenOrCreate, null, m_ContentsMMFSize);
            m_ContentsViewAccessor = m_ContentsMMF.CreateViewAccessor();
        }

        public void Close()
        {
            Flush();

            m_HeadersViewAccessor?.Dispose();
            m_HeadersMMF?.Dispose();

            m_ContentsViewAccessor?.Dispose();
            m_ContentsMMF?.Dispose();
        }

        public void Flush()
        {
            m_HeadersViewAccessor?.Flush();
            m_ContentsViewAccessor?.Flush();
        }

        public void DeleteAllKeys()
        {
            foreach (var header in m_Headers.KeyValueHeaders)
            {
                header.UpdateAndWriteKeyValueFlag(m_HeadersViewAccessor, KeyValueFlag.Delete);
            }
            UpdateHeaderChecksumHash();
        }

        public bool ContainsKey(string key)
        {
            return m_Contents.TryGetContentData(key, out var _);
        }

        public void DeleteKey(string key)
        {
            if (m_Contents.TryGetContentData(key, out KeyValueBody keyValueBody))
            {
                keyValueBody.Header.UpdateAndWriteKeyValueFlag(m_HeadersViewAccessor, KeyValueFlag.Delete);
                UpdateHeaderChecksumHash();
            }
        }

        public void SetInt(string key, int value)
        {
            SetValue(key, BitConverter.GetBytes(value));
        }
        public int GetInt(string key, int defaultValue = default(int))
        {
            return GetValue<int>(key, Converter.ToInt, defaultValue);
        }

        public void SetBool(string key, bool value)
        {
            SetValue(key, BitConverter.GetBytes(value));
        }
        public bool GetBool(string key, bool defaultValue = default(bool))
        {
            return GetValue<bool>(key, Converter.ToBool, defaultValue);
        }

        public void SetLong(string key, long value)
        {
            SetValue(key, BitConverter.GetBytes(value));
        }
        public long GetLong(string key, long defaultValue = default(long))
        {
            return GetValue<long>(key, Converter.ToLong, defaultValue);
        }

        public void SetFloat(string key, float value)
        {
            SetValue(key, BitConverter.GetBytes(value));
        }
        public float GetFloat(string key, float defaultValue = default(float))
        {
            return GetValue<float>(key, Converter.ToFloat, defaultValue);
        }

        public void SetDouble(string key, double value)
        {
            SetValue(key, BitConverter.GetBytes(value));
        }
        public double GetDouble(string key, double defaultValue = default(double))
        {
            return GetValue<double>(key, Converter.ToDouble, defaultValue);
        }

        public void SetString(string key, string value)
        {
            SetValue(key, value == null ? null : Encoding.UTF8.GetBytes(value));
        }
        public string GetString(string key, string defaultValue = null)
        {
            return GetValue<string>(key, Converter.ToChars, defaultValue);
        }

        public void SetBytes(string key, byte[] value)
        {
            if (value == null)
            {
                SetValue(key, null);
            }
            else
            {
                byte[] input = new byte[value.Length];
                Buffer.BlockCopy(value, 0, input, 0, value.Length);
                SetValue(key, input);
            }
        }
        public byte[] GetBytes(string key, byte[] defaultValue = null)
        {
            return GetValue<byte[]>(key, Converter.ToBytes, defaultValue);
        }

        public void SetValue(string key, byte[] value)
        {
#if FAST_KV_PROFILE
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
#endif
            bool isNullValue = value == null;
            value = value ?? Array.Empty<byte>();
            EncryptData(value);
            bool didUpdateValue = SetValueInternal(key, value, isNullValue);
#if FAST_KV_PROFILE
            stopwatch.Stop();
            if (didUpdateValue) Debug.Log($"[FastKV] Set value time: {stopwatch.ElapsedTicks * 1f / TimeSpan.TicksPerMillisecond}");
#endif
        }

        private bool SetValueInternal(string key, byte[] value, bool isNullValue)
        {
            if (m_Contents.TryGetContentData(key, out KeyValueBody keyValueBody))
            {
                if (isNullValue)
                {
                    if (keyValueBody.Header.Flag != KeyValueFlag.Null)
                    {
                        keyValueBody.Header.UpdateAndWriteKeyValueFlag(m_HeadersViewAccessor, KeyValueFlag.Null);
                        UpdateHeaderChecksumHash();
                        
                    }
                    return false;
                }

                if (keyValueBody.ValueSize == value.Length)
                {
                    if (keyValueBody.Header.Flag != KeyValueFlag.Normal)
                    {
                        keyValueBody.Header.UpdateAndWriteKeyValueFlag(m_HeadersViewAccessor, KeyValueFlag.Normal);
                        UpdateHeaderChecksumHash();
                    }

                    if (keyValueBody.DidReadValue && value.Length <= 8 && value.IsSequenceEqual(keyValueBody.Value))
                    {
                        return false;
                    }
                    keyValueBody.WriteValue(m_ContentsViewAccessor, value);
                    return true;
                }
                else
                {
                    keyValueBody.Header.UpdateAndWriteKeyValueFlag(m_HeadersViewAccessor, KeyValueFlag.Delete);
                }
            }

            var newKeyValueHeader = new KeyValueHeader();
            newKeyValueHeader.Flag = isNullValue ? KeyValueFlag.Null : KeyValueFlag.Normal;
            newKeyValueHeader.KeySize = (byte)Encoding.UTF8.GetByteCount(key);
            newKeyValueHeader.Key = key;
            newKeyValueHeader.BodySize = value.Length + m_ValueHashAlgorithm.HashSize / 8;
            newKeyValueHeader.CalculateHeaderSize();

            if (m_Headers.HeaderActualSize + newKeyValueHeader.HeaderSize > m_HeadersMMFSize)
            {
                CleanMemoryMappedFiles();
            }
            if (m_Headers.HeaderActualSize + newKeyValueHeader.HeaderSize > m_HeadersMMFSize)
            {
                ExpandHeaderMemoryMappedFile();
            }
            if (m_Contents.ContentsActualSize + newKeyValueHeader.BodySize > m_ContentsMMFSize)
            {
                ExpandContentsMemoryMappedFile(m_Contents.ContentsActualSize + newKeyValueHeader.BodySize);
            }
            // CleanMemoryMappedFile之后，需要重新设置ValueOffset
            newKeyValueHeader.BodyOffset = m_Contents.ContentsActualSize;

            m_Headers.KeyValueHeaders.Add(newKeyValueHeader);
            m_Headers.HeaderCount = (ushort)m_Headers.KeyValueHeaders.Count;
            m_HeadersViewAccessor.Write(m_Headers.HeaderCountPosition, m_Headers.HeaderCount);
            m_Headers.HeaderActualSize = newKeyValueHeader.FullWriteToMemoryMappedFile(m_HeadersViewAccessor, m_Headers.HeaderActualSize);
            UpdateHeaderChecksumHash();

            var newkeyValueBody = new KeyValueBody(m_ValueHashAlgorithm, newKeyValueHeader);
            newkeyValueBody.WriteValue(m_ContentsViewAccessor, value);
            m_Contents.AddContentData(key, newkeyValueBody);
            // Debug.Log($"[FastKV] Will set new {key} to {value} at offset {keyValue.Header.ValueOffset} with size {keyValue.Header.ValueSize} ==> ViewAccessorCapacity:{m_ContentViewAccessor.Capacity}");

            return true;
        }

        public T GetValue<T>(string key, Func<byte[], T> converter, T defaultValue)
        {
#if FAST_KV_PROFILE
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
#endif
            T result = defaultValue;
            var bytes = GetValueInternal(key);
            if (bytes != null)
            {
                DecryptData(bytes);
                result = converter(bytes);
                EncryptData(bytes);
            }
#if FAST_KV_PROFILE
            stopwatch.Stop();
            // Debug.Log($"[FastKV] Get value time: {stopwatch.ElapsedTicks * 1f / TimeSpan.TicksPerMillisecond}");
#endif
            return result;
        }
        private byte[] GetValueInternal(string key)
        {
            if (m_Contents.TryGetContentData(key, out KeyValueBody keyValueBody))
            {
                if (keyValueBody.Header.Flag == KeyValueFlag.Null)
                {
                    return null;
                }

                if (!keyValueBody.DidReadValue)
                {
                    keyValueBody.ReadValue(m_ContentsViewAccessor);
                }
                return keyValueBody.Value;
            }
            else
            {
                return null;
            }
        }

        private byte[] EncryptData(byte[] bytes) => ProcessByteCodes(bytes);
        private byte[] DecryptData(byte[] bytes) => ProcessByteCodes(bytes);
        private byte[] ProcessByteCodes(byte[] bytes)
        {
            if (bytes == null) return null;
            for (int i = 0; i < bytes.Length; i++)
            {
                bytes[i] ^= m_EncryptionKey[i % m_EncryptionKey.Length];
            }
            return bytes;
        }

    }
}

