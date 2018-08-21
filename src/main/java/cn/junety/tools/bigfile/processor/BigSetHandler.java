package cn.junety.tools.bigfile.processor;

import java.util.Set;

/**
 * Created by caijt on 2018/8/17
 */
public interface BigSetHandler {

    /**
     * 对两个集合进行运算
     *
     * @param set1 集合1
     * @param set2 集合2
     */
    Set<String> handle(Set<String> set1, Set<String> set2);
}
