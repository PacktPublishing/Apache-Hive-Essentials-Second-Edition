package com.packtpub.hive.essentials.hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;

@Description(
	     name = "aesencrypt",
	     value = "_FUNC_(str) - Returns encrypted string based on AES key.",
	     extended = "Example:\n" +
	     "  > SELECT aesencrypt(pii_info) FROM table_name;\n"
	     )
@UDFType(deterministic = true, stateful = false)
/*
 * A Hive encryption UDF
 */
public class AESEncrypt extends UDF {
    public String evaluate(String unencrypted) {
    String encrypted="";
	if(unencrypted != null) {
	    try {
	    	encrypted = CipherUtils.encrypt(unencrypted);
	    } catch (Exception e) {};
	}
	return encrypted;
    }
}