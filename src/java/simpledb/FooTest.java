package simpledb;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class FooTest {
    public static void main(String[] args){
//        byte b = (byte)0b00111011;
//        byte mask = 1<<3;
//
////        b <<= 5;
////        int num=0;
////        while (b != 0)
////        {
////            b &=(b-1);
////            num++;
////        }
//        System.out.println((b&mask) == (byte)0);

//        ArrayList<String> sites = new ArrayList<>();
//        sites.add("Google");
//        sites.add("Runoob");
//        sites.add("Taobao");
//        sites.add("Zhihu");
//
//        // 获取迭代器
//        int i =0;
//        System.out.println(sites);
        Set<String> ss = new HashSet<>();
        ss.add("a");
        ss.add("b");
        System.out.println(ss.size());
    }
}
