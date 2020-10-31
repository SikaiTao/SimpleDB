package simpledb;
import java.util.*;

public class FooTest {
    public static void main(String[] args){
        Map map = new HashMap();
        map.put(null, null);
//        map.put(null, null);
        map.put("3", null);
        System.out.println(map.get(null));
        System.out.println(map.size());

    }
}
