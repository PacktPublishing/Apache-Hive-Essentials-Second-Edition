package com.packtpub.hive.essentials.hiveudf;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
/*
 * The core encryption and decryption logic
 */
public class CipherUtils
{
	//This is a secret key in terms of ASCII
    private static byte[] key = {
    	0x75, 0x69, 0x69, 0x73, 0x40, 0x73, 0x41, 0x53, 0x65, 0x65, 0x72, 0x69, 0x74, 0x4b, 0x65, 0x75          
    };

    public static String encrypt(String strToEncrypt)
    {
        try
        {
        	//prepare algorithm
        	Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            final SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            //initialize cipher for encryption
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            //Base64.encodeBase64String that gives an ascii string
            final String encryptedString = Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes()));
            return encryptedString.replaceAll("\r|\n", "");
        }
        catch (Exception e)
        {
           e.printStackTrace();
        }
        return null;

    }

    public static String decrypt(String strToDecrypt)
    {
        try
        {
            //prepare algorithm
        	Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            final SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            //initialize cipher for decryption
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            final String decryptedString = new String(cipher.doFinal(Base64.decodeBase64(strToDecrypt)));
            return decryptedString;
        }
        catch (Exception e)
        {
        	 e.printStackTrace();

        }
        return null;
    }
//    public static void main(String args[])
//    {
//        final String strToEncrypt = "Toronto";
//        final String encryptedStr = CipherUtils.encrypt(strToEncrypt.trim());
//        System.out.println("String to Encrypt : " + strToEncrypt);
//        System.out.println("Encrypted : " + encryptedStr);
//        System.out.println("Encrypted END ");
//       
//        final String strToDecrypt = encryptedStr;
//        final String decryptedStr = CipherUtils.decrypt(strToDecrypt.trim());
//        System.out.println("String To Decrypt : " + strToDecrypt);
//        System.out.println("Decrypted : " + decryptedStr);
//        System.out.println("Decrypted END ");
//         
//    }
}
