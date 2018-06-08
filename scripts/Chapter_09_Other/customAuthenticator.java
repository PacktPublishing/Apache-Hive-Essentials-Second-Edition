package com.packtpub.hive.essentials.hiveudf;

import java.util.Hashtable;
import javax.security.sasl.AuthenticationException;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

/*
 * The customized class for HiveServer2 authentication
 */

public class customAuthenticator implements PasswdAuthenticationProvider {

  Hashtable<String, String> authHashTable = null;

  public customAuthenticator () {
	  authHashTable = new Hashtable<String, String>();
	  authHashTable.put("user1", "passwd1");
	  authHashTable.put("user2", "passwd2");
  }

  @Override
  public void Authenticate(String user, String password) 
		  throws AuthenticationException {

    String storedPasswd = authHashTable.get(user);

    if (storedPasswd != null && storedPasswd.equals(password)) 
    	return;

    throw new AuthenticationException("customAuthenticator Exception: Invalid user");
  }

}