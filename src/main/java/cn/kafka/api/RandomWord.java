package cn.kafka.api;

import java.util.Random;

/**
 * @author zhangjin
 * @create 2018-06-19 17:54
 */
public class RandomWord {


    public static String getRandomWord() {
        int assicode = new Random().nextInt(26) + 97;
        char word = (char) assicode;
        return word + "";
    }

    public static void main(String[] args) throws InterruptedException {

        while (true) {

            int assicode = new Random().nextInt(26) + 97;
            char word = (char) assicode;
            System.out.println(word);
            Thread.sleep(100);
        }


    }
}
