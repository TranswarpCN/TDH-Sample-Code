package io.transwarp.kafkaConsumer;

import kafka.auth.AuthenticationManager;

import java.util.Properties;

public class kafkaProperties {
    private Properties properties = new Properties();

    // 加载kafka配置
    public kafkaProperties() {
        Constant constant = new Constant();

        if (constant.OPEN_KERBEROS.equals("true")) {
            AuthenticationManager.setAuthMethod("kerberos");
            AuthenticationManager.login(constant.KERBEROS_USER, constant.KEYTAB);
        }

        properties.put("zookeeper.connect", constant.ZK_CONNECT);
        properties.put("group.id", constant.GROUP_ID);
        properties.put("zookeeper.session.timeout.ms", "400");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
    }

    public Properties properties() {
        return properties;
    }
}
