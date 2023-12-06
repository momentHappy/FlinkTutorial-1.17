package com.atguigu;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

/**
 * Unit test for simple App.
 */
public class AppTest {

    public static void main(String[] args) {

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        int result = compiler.run(null, null, null, "D:\\java_code\\FlinkTutorial-1.17\\src\\main\\java\\org\\atguigu\\wc\\WordCountStreamDemo.java");   // 在 D:\\Test.java 同目录下，会出现Test.class文件
        System.out.println(result == 0 ? "编译成功" : "编译失败");

    }

}
