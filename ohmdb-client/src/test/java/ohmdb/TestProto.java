package ohmdb;

import ohmdb.client.generated.ClientProtos;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: alex
 * Date: 5/22/13
 * Time: 12:26 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestProto {


  @Test
  public void testClientProto() {
    ClientProtos.Call call = ClientProtos.Call.getDefaultInstance();
    call.toString();
  }
}
