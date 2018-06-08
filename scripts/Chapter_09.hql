--Apache Hive Essentials
--Chapter 9 Code - Security Considerations

--Use hash function
SELECT 
name, 
md5(name) as md5_name, -- 128 bit
sha1(name) as sha1_name, -- 160 bit
sha2(name, 256) as sha2_name -- 256 bit
FROM employee;

--Use data mask udf
select 
mask("Card-0123-4567-8910", "U", "l", "#") as m0, 
mask_first_n("Card-0123-4567-8910", 4) as m1, 
mask_last_n("Card-0123-4567-8910", 4) as m2,
mask_show_first_n("Card-0123-4567-8910", 4) as m3,
mask_show_last_n("Card-0123-4567-8910", 4) as m4,
mask_hash('Card-0123-4567-8910') as m5;

--Use built-in UDF for encryption/decryption
SELECT
name,
aes_encrypt(name,'1234567890123456') as encrypted,
aes_decrypt(
aes_encrypt(name,'1234567890123456'),
'1234567890123456') as decrypted
FROM employee;

--Use encryption and decryption as 3rd party UDF 
ADD JAR /home/dayongd/Downloads/hiveessentials-1.0-SNAPSHOT.jar;                    

CREATE TEMPORARY FUNCTION aesencrypt AS 'com.packtpub.hive.essentials.hiveudf.AESEncrypt';
CREATE TEMPORARY FUNCTION aesdecrypt AS 'com.packtpub.hive.essentials.hiveudf.AESDecrypt';

SELECT aesencrypt('Will') AS encrypt_name FROM employee LIMIT 1;                         
SELECT aesdecrypt('YGvo54QIahpb+CVOwv9OkQ==') AS decrypt_name FROM employee LIMIT 1;   
