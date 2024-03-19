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
        fileSystem.mkdir("/user/xiongtaolong/one");
        fileSystem.mkdir("/user/xiongtaolong/two");
    }
}
