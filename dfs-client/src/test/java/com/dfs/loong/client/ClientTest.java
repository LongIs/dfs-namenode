package com.dfs.loong.client;

import com.dfs.loong.DfsClientApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = DfsClientApplication.class)
@RunWith(SpringRunner.class)
public class ClientTest {

    @Autowired
    private FileSystem fileSystem;

    @Test
    public void mkdir () {
        for (int i = 0; i < 300; i++) {
            int finalI = i;
            Thread thread = new Thread(() -> {

                for (int j = 0; j < 5; j++) {
                    try {
                        fileSystem.mkdir("/user/xiongtaolong/one" +j +"_" + finalI);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            thread.start();

            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void shutdown() {
        fileSystem.shutdown();
    }
}
