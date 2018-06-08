package com.packtpub.hive.essentials.hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;

@Description(
	     name = "aesdecrypt",
	     value = "_FUNC_(str) - Returns unencrypted string based on AES key.",
	     extended = "Example:\n" +
	     "  > SELECT aesdecrypt(pii_info) FROM table_name;\n"
	     )
@UDFType(deterministic = true, stateful = false)
/*
 * A Hive decryption UDF
 */
public class AESDecrypt extends UDF {
    public String evaluate(String encrypted) {
    String unencrypted = new String(encrypted);
	if(encrypted != null) {
	    try {
	    	unencrypted = CipherUtils.decrypt(encrypted);
	    } catch (Exception e) {};
	}
	return unencrypted;
    }
}