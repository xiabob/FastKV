using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using UnityEngine;
using UnityEngine.TestTools;
using xiabob.FastKV;
using System.IO;
using UnityEngine.Profiling;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;

class MD5Hash : FastKV.IHashAlgorithm
{
    private MD5 m_MD5;

    public MD5Hash() => m_MD5 = MD5.Create();

    public int HashSize => m_MD5.HashSize;
    public byte[] ComputeHash(byte[] buffer, int offset, int count) => m_MD5.ComputeHash(buffer, offset, count);
}

class XORChecksumAlgorithm : FastKV.IHashAlgorithm
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

        int blockSize = 32;
        int loopCount = count / blockSize;
        int i = 0;
        for (int loop = 0; loop < loopCount; loop++)
        {
            i = offset + loop * blockSize;
            m_Buffer[0] ^= buffer[i];
            m_Buffer[0] ^= buffer[i + 1];
            m_Buffer[0] ^= buffer[i + 2];
            m_Buffer[0] ^= buffer[i + 3];
            m_Buffer[0] ^= buffer[i + 4];
            m_Buffer[0] ^= buffer[i + 5];
            m_Buffer[0] ^= buffer[i + 6];
            m_Buffer[0] ^= buffer[i + 7];
            m_Buffer[0] ^= buffer[i + 8];
            m_Buffer[0] ^= buffer[i + 9];
            m_Buffer[0] ^= buffer[i + 10];
            m_Buffer[0] ^= buffer[i + 11];
            m_Buffer[0] ^= buffer[i + 12];
            m_Buffer[0] ^= buffer[i + 13];
            m_Buffer[0] ^= buffer[i + 14];
            m_Buffer[0] ^= buffer[i + 15];
            m_Buffer[0] ^= buffer[i + 16];
            m_Buffer[0] ^= buffer[i + 17];
            m_Buffer[0] ^= buffer[i + 18];
            m_Buffer[0] ^= buffer[i + 19];
            m_Buffer[0] ^= buffer[i + 20];
            m_Buffer[0] ^= buffer[i + 21];
            m_Buffer[0] ^= buffer[i + 22];
            m_Buffer[0] ^= buffer[i + 23];
            m_Buffer[0] ^= buffer[i + 24];
            m_Buffer[0] ^= buffer[i + 25];
            m_Buffer[0] ^= buffer[i + 26];
            m_Buffer[0] ^= buffer[i + 27];
            m_Buffer[0] ^= buffer[i + 28];
            m_Buffer[0] ^= buffer[i + 29];
            m_Buffer[0] ^= buffer[i + 30];
            m_Buffer[0] ^= buffer[i + 31];
            i += blockSize;
        }
        for (; i < offset + count; i++)
        {
            m_Buffer[0] ^= buffer[i];
        }
        return m_Buffer;
    }
}

public class FastKVTest
{

    private FastKV m_TestFastKV;

    [SetUp]
    public void SetUp()
    {
        // m_TestFastKV = FastKV.Open("UTF_Test", "1234abcd", new MD5Hash());
        m_TestFastKV = FastKV.Open("UTF_Test", "1234abcd", new XORChecksumAlgorithm(), null);
    }

    [TearDown]
    public void Teardown()
    {
        m_TestFastKV.Close();
        var folder = Path.Combine(Application.persistentDataPath, "FastKV");
        Directory.Delete(folder, true);
    }

    [Test]
    public void TestCheckHeaderHash()
    {
        m_TestFastKV.SetBool("test_bool", true);
        Assert.AreEqual(m_TestFastKV.GetBool("test_bool"), true);

        m_TestFastKV.SetInt("test_int", 1);
        Assert.AreEqual(m_TestFastKV.GetInt("test_int"), 1);

        m_TestFastKV.SetString("test_string", "test_string");
        Assert.AreEqual(m_TestFastKV.GetString("test_string"), "test_string");

        m_TestFastKV.SetDouble("test_double", 1.123456789100001);
        Assert.AreEqual(m_TestFastKV.GetDouble("test_double"), 1.123456789100001);

        m_TestFastKV.SetLong("test_long", 123448271319381983);
        Assert.AreEqual(m_TestFastKV.GetLong("test_long"), 123448271319381983);

        m_TestFastKV.Close();
        var path = Path.Combine(Path.Combine(Application.persistentDataPath, "FastKV"), "UTF_Test.meta");
        var stream = File.OpenWrite(path);
        stream.Seek(stream.Length - 1, SeekOrigin.Begin);
        stream.WriteByte(0xff);
        stream.Dispose();

        SetUp();
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), false);

        m_TestFastKV.Close();
        SetUp();
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), false);
    }

    [Test]
    public void TestCheckValueHash()
    {
        m_TestFastKV.SetBool("test_bool", true);
        Assert.AreEqual(m_TestFastKV.GetBool("test_bool"), true);

        m_TestFastKV.SetInt("test_int", 1);
        Assert.AreEqual(m_TestFastKV.GetInt("test_int"), 1);

        m_TestFastKV.SetString("test_string", "test_string");
        Assert.AreEqual(m_TestFastKV.GetString("test_string"), "test_string");

        m_TestFastKV.SetDouble("test_double", 1.123456789100001);
        Assert.AreEqual(m_TestFastKV.GetDouble("test_double"), 1.123456789100001);

        m_TestFastKV.SetLong("test_long", 123448271319381983);
        Assert.AreEqual(m_TestFastKV.GetLong("test_long"), 123448271319381983);

        m_TestFastKV.Close();
        var path = Path.Combine(Path.Combine(Application.persistentDataPath, "FastKV"), "UTF_Test");
        var stream = File.OpenWrite(path);
        stream.Seek(12, SeekOrigin.Begin);
        stream.WriteByte(0xf0);
        stream.Dispose();

        SetUp();
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), false);

        m_TestFastKV.Close();
        SetUp();
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), false);
    }

    [Test]
    public void TestContainsKey()
    {
        m_TestFastKV.SetBool("test_bool", true);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), true);

        m_TestFastKV.SetInt("test_int", 1);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), true);

        m_TestFastKV.SetString("test_string", "test_string");
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), true);

        m_TestFastKV.SetString("test_string", string.Empty);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), true);

        m_TestFastKV.SetString("test_string", null);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), true);

        m_TestFastKV.SetDouble("test_double", 1.123456789100001);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), true);

        m_TestFastKV.SetLong("test_long", 1);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), true);

        m_TestFastKV.Close();
        SetUp();
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), true);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), true);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), true);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), true);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), true);
    }

    [Test]
    public void TestDeleteKey()
    {
        m_TestFastKV.SetBool("test_bool", true);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), true);
        m_TestFastKV.DeleteKey("test_bool");
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), false);

        m_TestFastKV.SetInt("test_int", 1);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), true);
        m_TestFastKV.DeleteKey("test_int");
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), false);

        m_TestFastKV.SetString("test_string", "test_string");
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), true);
        m_TestFastKV.DeleteKey("test_string");
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), false);

        m_TestFastKV.SetDouble("test_double", 1);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), true);
        m_TestFastKV.DeleteKey("test_double");
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), false);

        m_TestFastKV.SetLong("test_long", 1);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), true);
        m_TestFastKV.DeleteKey("test_long");
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), false);

        m_TestFastKV.Close();
        SetUp();
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), false);
    }

    [Test]
    public void TestDeleteAllKeys()
    {
        m_TestFastKV.SetBool("test_bool", true);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), true);

        m_TestFastKV.SetInt("test_int", 1);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), true);

        m_TestFastKV.SetString("test_string", "test_string");
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), true);

        m_TestFastKV.SetDouble("test_double", 1);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), true);

        m_TestFastKV.SetLong("test_long", 1);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), true);

        m_TestFastKV.DeleteAllKeys();
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), false);

        m_TestFastKV.Close();
        SetUp();
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_bool"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_int"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_string"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_double"), false);
        Assert.AreEqual(m_TestFastKV.ContainsKey("test_long"), false);
    }

    [TestCase(false, ExpectedResult = false)]
    [TestCase(true, ExpectedResult = true)]
    public bool TestBool(bool value)
    {
        m_TestFastKV.SetBool("test_bool", value);
        Assert.AreEqual(m_TestFastKV.GetBool("test_bool"), value);

        m_TestFastKV.Close();
        SetUp();
        return m_TestFastKV.GetBool("test_bool");
    }

    [TestCase(int.MinValue, ExpectedResult = int.MinValue)]
    [TestCase(int.MaxValue, ExpectedResult = int.MaxValue)]
    [TestCase(0, ExpectedResult = 0)]
    [TestCase(1, ExpectedResult = 1)]
    [TestCase(2, ExpectedResult = 2)]
    [TestCase(2049, ExpectedResult = 2049)]
    [TestCase(-1, ExpectedResult = -1)]
    [TestCase(-2, ExpectedResult = -2)]
    [TestCase(-577, ExpectedResult = -577)]
    public int TestInt(int value)
    {
        m_TestFastKV.SetInt("test_int", value);
        Assert.AreEqual(m_TestFastKV.GetInt("test_int"), value);

        m_TestFastKV.Close();
        SetUp();
        return m_TestFastKV.GetInt("test_int");
    }

    [TestCase(long.MaxValue, ExpectedResult = long.MaxValue)]
    [TestCase(long.MinValue, ExpectedResult = long.MinValue)]
    [TestCase(0L, ExpectedResult = 0L)]
    [TestCase(1L, ExpectedResult = 1L)]
    [TestCase(136978203L, ExpectedResult = 136978203L)]
    [TestCase(9223372036855807, ExpectedResult = 9223372036855807)]
    [TestCase(-9L, ExpectedResult = -9L)]
    [TestCase(-131313131789, ExpectedResult = -131313131789)]
    [TestCase(-9301481313131789, ExpectedResult = -9301481313131789)]
    public long TestLong(long value)
    {
        m_TestFastKV.SetLong("test_long", value);
        Assert.AreEqual(m_TestFastKV.GetLong("test_long"), value);

        m_TestFastKV.Close();
        SetUp();
        return m_TestFastKV.GetLong("test_long");
    }

    [TestCase(float.MaxValue, ExpectedResult = float.MaxValue)]
    [TestCase(float.MinValue, ExpectedResult = float.MinValue)]
    [TestCase(0, ExpectedResult = 0)]
    [TestCase(1.1234f, ExpectedResult = 1.1234f)]
    [TestCase(98065112222371.1234f, ExpectedResult = 98065112222371.1234f)]
    [TestCase(-9.999123456789999f, ExpectedResult = -9.999123456789999f)]
    public float TestFloat(float value)
    {
        m_TestFastKV.SetFloat("test_float", value);
        Assert.AreEqual(m_TestFastKV.GetFloat("test_float"), value);

        m_TestFastKV.Close();
        SetUp();
        return m_TestFastKV.GetFloat("test_float");
    }

    [TestCase(double.MaxValue, ExpectedResult = double.MaxValue)]
    [TestCase(double.MinValue, ExpectedResult = double.MinValue)]
    [TestCase(0, ExpectedResult = 0)]
    [TestCase(1.1234, ExpectedResult = 1.1234)]
    [TestCase(123456789100001, ExpectedResult = 123456789100001)]
    [TestCase(1234567891000013179138719389138d, ExpectedResult = 1234567891000013179138719389138d)]
    [TestCase(1234567891000013179138719389138.371846284729489148d, ExpectedResult = 1234567891000013179138719389138.371846284729489148d)]
    [TestCase(-9.9991234567899993211101f, ExpectedResult = -9.9991234567899993211101f)]
    [TestCase(-1234567891000013179138719389138.371846284729489148d, ExpectedResult = -1234567891000013179138719389138.371846284729489148d)]
    public double TestDouble(double value)
    {
        m_TestFastKV.SetDouble("test_double", value);
        Assert.AreEqual(m_TestFastKV.GetDouble("test_double"), value);

        m_TestFastKV.Close();
        SetUp();
        return m_TestFastKV.GetDouble("test_double");
    }

    [TestCase(null, ExpectedResult = null)]
    [TestCase("", ExpectedResult = "")]
    [TestCase(" ", ExpectedResult = " ")]
    [TestCase("test", ExpectedResult = "test")]
    [TestCase(" 1234test真滴啊打火机大家", ExpectedResult = " 1234test真滴啊打火机大家")]
    [TestCase("emoji😈😁🚩🧑‍💻🧑‍💼🦶🤱🚠🏄🥕🦆🇧🇾🇧🇲", ExpectedResult = "emoji😈😁🚩🧑‍💻🧑‍💼🦶🤱🚠🏄🥕🦆🇧🇾🇧🇲")]
    public string TestString(string value)
    {
        m_TestFastKV.SetString("test_string", Random.Range(float.MinValue, float.MaxValue).ToString());
        m_TestFastKV.SetString("test_string", value);
        Assert.AreEqual(m_TestFastKV.GetString("test_string"), value);

        m_TestFastKV.SetString("test_string_empty", "xx");
        Assert.AreEqual(m_TestFastKV.GetString("test_string_empty"), "xx");
        m_TestFastKV.Close();
        SetUp();

        m_TestFastKV.SetString("test_string_empty", null);
        m_TestFastKV.Close();
        SetUp();
        Assert.AreEqual(m_TestFastKV.GetString("test_string_empty"), null);

        m_TestFastKV.SetString("test_string_empty", string.Empty);
        m_TestFastKV.Close();
        SetUp();
        Assert.AreEqual(m_TestFastKV.GetString("test_string_empty"), string.Empty);

        m_TestFastKV.Close();
        SetUp();
        Assert.AreEqual(m_TestFastKV.GetString("test_string_empty"), string.Empty);
        return m_TestFastKV.GetString("test_string");
    }

    [TestCase(null, ExpectedResult = null)]
    [TestCase(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, ExpectedResult = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 })]
    public byte[] TestBytes(byte[] value)
    {
        m_TestFastKV.SetBytes("test_bytes", value);
        Assert.AreEqual(m_TestFastKV.GetBytes("test_bytes"), value);

        m_TestFastKV.Close();
        SetUp();
        return m_TestFastKV.GetBytes("test_bytes");
    }

    [System.Serializable]
    class Model
    {
        public bool A;
        public int B;
        public long C;
        public float D;
        public double E;
        public string F;
        public Model2 M;


        public Model()
        {
            A = true;
            B = 123;
            C = 123456789L;
            D = 3.14f;
            E = 123456789100001;
            F = "test";
            M = new Model2();
        }

    }
    [System.Serializable]
    class Model2
    {
        public List<object> List;

        public Model2()
        {
            List = new List<object>();
            List.Add(false);
            List.Add(123);
            List.Add(123456789L);
            List.Add(3.14f);
            List.Add(123456789100001);
            List.Add("test🙃😝🤓🤓🤓🤓☎️💿💶💷🙆‍♂️");
        }
    }
    [Test]
    public void TestBytesSerialize()
    {
        Model m = new Model();
        BinaryFormatter serializer = new BinaryFormatter();
        System.IO.MemoryStream memStream = new System.IO.MemoryStream();
        serializer.Serialize(memStream, m);
        m_TestFastKV.SetBytes("test_bytes_serialize", memStream.ToArray());
        Assert.AreEqual(m_TestFastKV.GetBytes("test_bytes_serialize"), memStream.ToArray());

        m_TestFastKV.Close();
        SetUp();
        Assert.AreEqual(m_TestFastKV.GetBytes("test_bytes_serialize"), memStream.ToArray());
    }

    [Test]
    public void TestSetAndGetValues()
    {
        // Profiler.BeginSample("FastKV");

        m_TestFastKV.SetBool("test_valuetype_Bool", true);
        m_TestFastKV.SetInt("test_valuetype_Int", 123);
        m_TestFastKV.SetFloat("test_valuetype_Float", 3.14f);
        m_TestFastKV.SetLong("test_valuetype_Long", 123456789L);
        m_TestFastKV.SetDouble("test_valuetype_Double", 123456789100001);

        Assert.AreEqual(m_TestFastKV.GetBool("test_valuetype_Bool"), true);
        Assert.AreEqual(m_TestFastKV.GetInt("test_valuetype_Int"), 123);
        Assert.AreEqual(m_TestFastKV.GetFloat("test_valuetype_Float"), 3.14f);
        Assert.AreEqual(m_TestFastKV.GetLong("test_valuetype_Long"), 123456789L);
        Assert.AreEqual(m_TestFastKV.GetDouble("test_valuetype_Double"), 123456789100001);

        m_TestFastKV.SetString("test_valuetype_String", "test");
        Assert.AreEqual(m_TestFastKV.GetString("test_valuetype_String"), "test");
        m_TestFastKV.SetString("test_valuetype_String", "test ");
        Assert.AreEqual(m_TestFastKV.GetString("test_valuetype_String"), "test ");
        m_TestFastKV.SetString("test_valuetype_String", " 1234test真滴啊打火机大家 emoji😈😁🚩🧑‍💻🧑‍💼🦶🤱🚠🏄🥕🦆🇧🇾🇧🇲 ");
        Assert.AreEqual(m_TestFastKV.GetString("test_valuetype_String"), " 1234test真滴啊打火机大家 emoji😈😁🚩🧑‍💻🧑‍💼🦶🤱🚠🏄🥕🦆🇧🇾🇧🇲 ");
        m_TestFastKV.SetString("test_valuetype_String", "kkldakAD 1234test真滴啊打火机大家 emoji😈😁🚩🧑‍💻🧑‍💼🦶🤱🚠🏄🥕🦆🇧🇾🇧🇲 ");
        Assert.AreEqual(m_TestFastKV.GetString("test_valuetype_String"), "kkldakAD 1234test真滴啊打火机大家 emoji😈😁🚩🧑‍💻🧑‍💼🦶🤱🚠🏄🥕🦆🇧🇾🇧🇲 ");

        m_TestFastKV.Close();
        SetUp();
        Assert.AreEqual(m_TestFastKV.GetBool("test_valuetype_Bool"), true);
        Assert.AreEqual(m_TestFastKV.GetInt("test_valuetype_Int"), 123);
        Assert.AreEqual(m_TestFastKV.GetFloat("test_valuetype_Float"), 3.14f);
        Assert.AreEqual(m_TestFastKV.GetLong("test_valuetype_Long"), 123456789L);
        Assert.AreEqual(m_TestFastKV.GetDouble("test_valuetype_Double"), 123456789100001);
        Assert.AreEqual(m_TestFastKV.GetString("test_valuetype_String"), "kkldakAD 1234test真滴啊打火机大家 emoji😈😁🚩🧑‍💻🧑‍💼🦶🤱🚠🏄🥕🦆🇧🇾🇧🇲 ");

        bool[] boolList = new bool[1111];
        int[] intList = new int[1111];
        float[] floatList = new float[1111];
        long[] longList = new long[1111];
        double[] doubleList = new double[1111];
        string[] stringList = new string[1111];

        for (int k = 0; k < 2; k++)
        {
            for (int i = 0; i < 1111; i++)
            {
                boolList[i] = Random.value >= 0.5;
                intList[i] = Random.Range(int.MinValue, int.MaxValue);
                floatList[i] = Random.Range(float.MinValue, float.MaxValue);
                longList[i] = Random.Range(int.MinValue, int.MaxValue);
                doubleList[i] = Random.Range(float.MinValue, float.MaxValue);

                m_TestFastKV.SetBool($"test_valuetype_Bool_{i}", boolList[i]);
                m_TestFastKV.SetInt($"test_valuetype_Int_{i}", intList[i]);
                m_TestFastKV.SetFloat($"test_valuetype_Float_{i}", floatList[i]);
                m_TestFastKV.SetLong($"test_valuetype_Long_{i}", longList[i]);
                m_TestFastKV.SetDouble($"test_valuetype_Double_{i}", doubleList[i]);

                for (int j = 0; j < 100; j++)
                {
                    stringList[i] = string.Empty;
                    if (Random.value < 0.25f)
                    {
                        stringList[i] = null;
                    }
                    else if (Random.value < 0.5f)
                    {
                        stringList[i] = string.Empty;
                    }
                    else
                    {
                        stringList[i] = Random.Range(int.MinValue, float.MaxValue).ToString();
                    }
                    m_TestFastKV.SetString($"test_valuetype_String_{i}", stringList[i]);
                    Assert.AreEqual(m_TestFastKV.GetString($"test_valuetype_String_{i}"), stringList[i]);
                }

                Assert.AreEqual(m_TestFastKV.GetBool($"test_valuetype_Bool_{i}"), boolList[i]);
                Assert.AreEqual(m_TestFastKV.GetInt($"test_valuetype_Int_{i}"), intList[i]);
                Assert.AreEqual(m_TestFastKV.GetFloat($"test_valuetype_Float_{i}"), floatList[i]);
                Assert.AreEqual(m_TestFastKV.GetLong($"test_valuetype_Long_{i}"), longList[i]);
                Assert.AreEqual(m_TestFastKV.GetDouble($"test_valuetype_Double_{i}"), doubleList[i]);
            }

            m_TestFastKV.Close();
            SetUp();
            for (int i = 0; i < 1111; i++)
            {
                Assert.AreEqual(m_TestFastKV.GetBool($"test_valuetype_Bool_{i}"), boolList[i]);
                Assert.AreEqual(m_TestFastKV.GetInt($"test_valuetype_Int_{i}"), intList[i]);
                Assert.AreEqual(m_TestFastKV.GetFloat($"test_valuetype_Float_{i}"), floatList[i]);
                Assert.AreEqual(m_TestFastKV.GetLong($"test_valuetype_Long_{i}"), longList[i]);
                Assert.AreEqual(m_TestFastKV.GetDouble($"test_valuetype_Double_{i}"), doubleList[i]);
                Assert.AreEqual(m_TestFastKV.GetString($"test_valuetype_String_{i}"), stringList[i]);
            }
        }

        // Profiler.EndSample();
    }

    // A Test behaves as an ordinary method
    // [Test]
    // public void FastKVTestSimplePasses()
    // {
    //     // Use the Assert class to test conditions
    // }

    // A UnityTest behaves like a coroutine in Play Mode. In Edit Mode you can use
    // `yield return null;` to skip a frame.
    // [UnityTest]
    // public IEnumerator FastKVTestWithEnumeratorPasses()
    // {
    //     // Use the Assert class to test conditions.
    //     // Use yield to skip a frame.
    //     yield return null;
    // }
}
