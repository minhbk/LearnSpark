import java.util.Arrays;
import java.util.List;

/**
 * Created by minh on 27/01/2017.
 */
public class Test {
    public static void main(String[] args) {
        String s = new String("");
        List<String> ar = Arrays.asList(s.split(" "));
        System.out.println(ar.get(0).equals(""));
    }
}
