# FastKV
A fast key value store library base on memory map files. You can use it in Unity.

## Usage
```
// open
fastKV = FastKV.Open("file_name", "xxxx");

// set and get
fastKV.SetInt("int_key");
fastKV.GetInt("int_key");

// after use you close it
fastKV.Close();

```
