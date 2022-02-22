package isc.rabbitmq;

public class Test {
    public static void main(String[] args) {

        try {

            String host = "localhost";
            int port = 5672;
            String user = "guest";
            String pass = "guest";
            String virtualHost = "/";
            String queue = "Test";

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

            // sendMessage
            APIMessage mesRequest = new APIMessage("text/xml", 1);

            // Headers
            mesRequest.setHeader("system", "MySystem");
            mesRequest.setHeader("node-request", "test.server");
            mesRequest.setHeader("service-name", "getClient");

            // Body
            mesRequest.setBodyString("Test message - русский текст");

            // MessageId
            mesRequest.MessageId = "00036";

            api.sendMessage(mesRequest);
            if (api.isLastError()) {
                System.out.println(api.getLastErrorMessage());
                api.close();
                return;
            }
/*
            // readMessage
            APIMessage mesResponse = api.readMessage();
            if (mesResponse == null || api.isLastError()) {
                System.out.println(api.getLastErrorMessage());
                api.close();
                return;
            }

            // output message response
            System.out.println(mesResponse);

        /*
        // BasicProperties
        String [] arProps = mesRequest.getProperties();
        if (arProps != null) {
            System.out.println("BasicProperties:");
            //for (String prop : arProps) System.out.println("    "+ prop);
            System.out.println(mesRequest.propertiesToString());
        }

        // Headers
        String [] arHeaders = mesRequest.getHeaders();
        if (arHeaders != null) {
            System.out.println("Headers:");
            //for (String header : arHeaders) System.out.println("    "+ header);
            System.out.println(mesRequest.headersToString());
        }

        // Body
        System.out.println("Body:");
        System.out.println("    "+ mesResponse.getBodyString());

*/
            api.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
