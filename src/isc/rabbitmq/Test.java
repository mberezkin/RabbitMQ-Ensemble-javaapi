package isc.rabbitmq;

import com.rabbitmq.client.impl.LongStringHelper;

public class Test {
    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 5672;
        String user = "guest";
        String pass = "guest";
        String virtualHost = "/";
        String queue = "Test";
        int durable = 0;

        // create connections and channel
        API api = new API(host, port, user, pass, virtualHost, queue);
        if (api.isLastError()) {
            System.out.println(api.getLastErrorMessage());
            api.close();
            return;
        }

        /*
        // queueDeclare
        if (api.queueDeclare(false, false, false)) {
            System.out.println(api.getLastErrorMessage());
            api.close();
            return;
        };

        // exchangeDeclare
        if (api.exchangeDeclare("direct", false, false, false)) {
            System.out.println(api.getLastErrorMessage());
            api.close();
            return;
        };
         */

        // sendMessageIdHeaders
        api.ContentType = "text/xml";
        api.DeliveryMode = 1;
        String[] headers = {"system=AnyWay", "node-request=test.anyway", "service-name=getClient"};

        api.sendMessageIdHeaders("Test message русский текст".getBytes("UTF8"), "", "0005", headers);
        if (api.isLastError()) {
            System.out.println(api.getLastErrorMessage());
            api.close();
            return;
        }

        // readMessageString
        String[] arHeaders = new String[5];
        String[] arProps = api.readMessageString(arHeaders);
        if (api.isLastError()) {
            System.out.println(api.getLastErrorMessage());
            api.close();
            return;
        }

        if (arProps != null) {
            System.out.println("Props");
            for (int i = 0; i < arProps.length; i++) {
                System.out.println("    "+ arProps[i]);
            }
        }

        System.out.println(" ");

        if (arHeaders != null) {
            System.out.println("Headers");
            for (int i = 0; i < arHeaders.length && arHeaders[i] != null; i++) {
                System.out.println("    "+ arHeaders[i]);
            }
        }

        api.close();

    }
}
